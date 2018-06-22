/*
 * Global Memory and Threading (GMT)
 *
 * Copyright © 2018, Battelle Memorial Institute
 * All rights reserved.
 *
 * Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to
 * any person or entity lawfully obtaining a copy of this software and associated
 * documentation files (hereinafter “the Software”) to redistribute and use the
 * Software in source and binary forms, with or without modification.  Such
 * person or entity may use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and may permit others to do
 * so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name `Battelle Memorial Institute` or `Battelle` may be used in
 *    any form whatsoever without the express written consent of `Battelle`.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL `BATTELLE` OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __QUEUE_H__
#define __QUEUE_H__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>
#include "gmt/utils.h"

#ifndef TEST_QUEUE
#include "gmt/debug.h"
#else
#define _assert assert
#define INLINE static inline
#endif

#define CACHE_LINE  128
#define FACTOR      8
#define MASK_ON (1l << 63)

/*******************************************************************
   There are special queues that exploit two properties:
  
   * We will never try to insert an element past their size.
     A check on this is not performed by the QUEUE.
   * On the MPMC, MPSC, SCMP stored values are at most 63-bit 
     (1 bit is used as flag to save a load on a volatile variable). 
     A check on this is performed by the QUEUE.
   
   This is the typical case of usage in the mechanism 
   queue/pool inside GMT used to pass around pointers to structures */

/*******************************************************************/

/*******************************************************************/
/*               multiple producers single consumer                */
/*******************************************************************/
#define DEFINE_QUEUE_MPSC(NAME,TYPE,SIZE)\
typedef struct NAME##_t {\
    int64_t writer_ticket           __align ( CACHE_LINE );\
    int64_t reader_ticket           __align ( CACHE_LINE );\
    TYPE    volatile * array        __align ( CACHE_LINE );\
} NAME##_t;\
\
INLINE void NAME##_init(NAME##_t * q) {\
    _assert(sizeof(TYPE) == 8 );\
    q->array = (TYPE volatile *) _malloc(NEXT_POW2(SIZE) * FACTOR * sizeof(TYPE));\
    _assert(q->array != NULL);\
    uint32_t i;\
    for(i =0; i < NEXT_POW2(SIZE); i++ )\
        q->array[i * FACTOR] = 0;\
    q->writer_ticket = 0;\
    q->reader_ticket = 0;\
}\
\
INLINE void NAME##_destroy(NAME##_t * q) {\
    free((void *) q->array);\
}\
\
INLINE void NAME##_push (NAME##_t * q, TYPE item) {\
    if((((long) item) & MASK_ON) != 0 ) { \
        printf("QUEUE insert error - only 63bit value supported\n"); \
        exit(1); \
    } \
    int ticket = __sync_fetch_and_add ( &q->writer_ticket, 1 );\
    int position = lower_bits (NEXT_POW2(SIZE), ticket );\
    q->array[position * FACTOR] = (TYPE) ((long) item | MASK_ON);\
}\
INLINE int NAME##_pop (NAME##_t * q, TYPE * item) {\
    int position = lower_bits (NEXT_POW2(SIZE), q->reader_ticket );\
    TYPE array_val = q->array[position * FACTOR];\
    if ( (((long)(array_val)) & MASK_ON) != MASK_ON )\
        return 0;\
    (*item) = (TYPE) (((long)(array_val)) & ~MASK_ON);\
    q->reader_ticket++;\
    q->array[position * FACTOR] = 0;\
    return 1;\
}

/*******************************************************************/
/*                single producer multiple consumers               */
/*******************************************************************/

#define DEFINE_QUEUE_SPMC(NAME,TYPE,SIZE)\
typedef struct NAME##_t {\
    int64_t writer_ticket           __align ( CACHE_LINE );\
    int64_t volatile reader_ticket  __align ( CACHE_LINE );\
    int64_t volatile lock           __align ( CACHE_LINE );\
    TYPE    volatile * array        __align ( CACHE_LINE );\
} NAME##_t;\
\
INLINE void NAME##_init(NAME##_t * q) {\
    _assert(sizeof(TYPE) == 8 );\
    q->array = (TYPE volatile *) _malloc(NEXT_POW2(SIZE) * FACTOR * sizeof(TYPE));\
    _assert(q->array != NULL);\
    uint32_t i;\
    for(i =0; i < NEXT_POW2(SIZE); i++ )\
        q->array[i * FACTOR] = 0;\
    q->writer_ticket = 0;\
    q->reader_ticket = 0;\
    q->lock = 0;\
}\
\
INLINE void NAME##_destroy(NAME##_t * q) {\
    free((void *) q->array);\
}\
\
INLINE void NAME##_push (NAME##_t * q, TYPE item) {\
    if((((long) item) & MASK_ON) != 0 ) { \
        printf("QUEUE insert error - only 63bit value supported\n"); \
        exit(1); \
    } \
    int position = lower_bits (NEXT_POW2(SIZE), q->writer_ticket );\
    q->writer_ticket++;\
    q->array[position * FACTOR] = (TYPE) ((long) item | MASK_ON);\
}\
INLINE int NAME##_pop (NAME##_t * q, TYPE * item) {\
    if (!__sync_bool_compare_and_swap (&q->lock, 0,1))\
        return 0;\
    int position = lower_bits (NEXT_POW2(SIZE), q->reader_ticket );\
    TYPE array_val = q->array[position * FACTOR];\
    if ( (((long)(array_val)) & MASK_ON) != MASK_ON ) {\
        q->lock = 0;\
        return 0;\
    }\
    (*item) = (TYPE) (((long)(array_val)) & ~MASK_ON);\
    q->array[position * FACTOR] = 0;\
    q->reader_ticket++;\
    q->lock = 0;\
    return 1;\
}

/*******************************************************************/
/*             multiple producer multiple consumer                 */
/*******************************************************************/

#define DEFINE_QUEUE_MPMC(NAME,TYPE,SIZE)\
typedef struct NAME##_t {\
    int64_t writer_ticket           __align ( CACHE_LINE );\
    int64_t volatile reader_ticket  __align ( CACHE_LINE );\
    char    volatile lock;      /*same cache lane as reader_ticket*/  \
    TYPE    volatile * array        __align ( CACHE_LINE );\
} NAME##_t;\
\
INLINE void NAME##_init(NAME##_t * q) {\
    _assert(sizeof(TYPE) == 8 );\
    q->array = (TYPE volatile *) _malloc(NEXT_POW2(SIZE) * FACTOR * sizeof(TYPE));\
    _assert(q->array != NULL);\
    uint32_t i;\
    for(i =0; i < NEXT_POW2(SIZE); i++ )\
        q->array[i * FACTOR] = 0;\
    q->writer_ticket = 0;\
    q->reader_ticket = 0;\
    q->lock = 0;\
}\
\
INLINE void NAME##_destroy(NAME##_t * q) {\
    free((void *) q->array);\
}\
\
INLINE void NAME##_push (NAME##_t * q, TYPE item) {\
    if((((long) item) & MASK_ON) != 0 ) { \
        printf("QUEUE insert error - only 63bit value supported\n"); \
        exit(1); \
    } \
    int ticket = __sync_fetch_and_add ( &q->writer_ticket, 1 );\
    int position = lower_bits (NEXT_POW2(SIZE), ticket );\
    q->array[position * FACTOR] = (TYPE) ((long) item | MASK_ON);\
}\
INLINE int NAME##_pop (NAME##_t * q, TYPE * item) {\
    if (!__sync_bool_compare_and_swap (&q->lock, 0,1))\
        return 0;\
    int position = lower_bits (NEXT_POW2(SIZE), q->reader_ticket );\
    TYPE array_val = q->array[position * FACTOR];\
    if ( (((long)(array_val)) & MASK_ON) != MASK_ON ) {\
        q->lock = 0;\
        return 0;\
    }\
    (*item) = (TYPE) (((long)(array_val)) & ~MASK_ON);\
    q->array[position * FACTOR] = 0;\
    q->reader_ticket++;\
    q->lock = 0;\
    return 1;\
}\
INLINE int64_t NAME##_guess_size (NAME##_t *q ) {\
    int64_t delta = (q->writer_ticket - q->reader_ticket);\
    return ( delta >= (int64_t)0 ) ? delta : (int64_t) (NEXT_POW2(SIZE) + delta);\
}


typedef struct qmpmc_t {
    int64_t writer_ticket __align(CACHE_LINE);
    int64_t volatile reader_ticket __align(CACHE_LINE);
    char volatile lock;         /*same cache lane as reader_ticket */
    void *volatile *array __align(CACHE_LINE);
    uint32_t size;
} qmpmc_t;

INLINE void qmpmc_init(qmpmc_t * q, uint32_t size)
{
    q->size = NEXT_POW2(size);
    q->array = (void *volatile *)_malloc(q->size * FACTOR * sizeof(void *));
    _assert(q->array != NULL);
    uint32_t i;
    for (i = 0; i < q->size; i++)
        q->array[i * FACTOR] = 0;
    q->writer_ticket = 0;
    q->reader_ticket = 0;
    q->lock = 0;
}

INLINE void qmpmc_destroy(qmpmc_t * q)
{
    free((void *)q->array);
}

INLINE void qmpmc_push(qmpmc_t * q, void *item)
{
    if ((((long)item) & MASK_ON) != 0) {
        printf("QUEUE insert error - only 63bit value supported\n");
        exit(1);
    }
    int ticket = __sync_fetch_and_add(&q->writer_ticket, 1);
    int position = lower_bits(q->size, ticket);
    q->array[position * FACTOR] = (void *)((long)item | MASK_ON);
}

INLINE void qmpmc_push_n(qmpmc_t * q, void **item, uint32_t n)
{
    int ticket = __sync_fetch_and_add(&q->writer_ticket, n);
    uint32_t i = 0;
    for (i = 0; i < n; i++) {
        if (((long)item[i] & MASK_ON) != 0) {
            printf("QUEUE insert error - only 63bit value supported\n");
            exit(1);
        }
        int position = lower_bits(q->size, (ticket + i));
        q->array[position * FACTOR] = (void *)((long)item[i] | MASK_ON);
    }
}

INLINE int qmpmc_pop(qmpmc_t * q, void **item)
{
    if (!__sync_bool_compare_and_swap(&q->lock, 0, 1)) 
        return 0;
    int position = lower_bits(q->size, q->reader_ticket);
    void *array_val = q->array[position * FACTOR];
    if ((((long)(array_val)) & MASK_ON) != MASK_ON) {
        q->lock = 0;
        return 0;
    }
    (*item) = (void *)(((long)(array_val)) & ~MASK_ON);
    q->array[position * FACTOR] = 0;
    q->reader_ticket++;
    q->lock = 0;
    return 1;
}

INLINE int qmpmc_pop_n(qmpmc_t * q, void **item, uint32_t n)
{
    if (!__sync_bool_compare_and_swap(&q->lock, 0, 1)) 
        return 0;
    uint32_t i = 0;
    for (i = 0; i < n; i++) {
        int position = lower_bits(q->size, q->reader_ticket);
        void *array_val = q->array[position * FACTOR];
        if ((((long)(array_val)) & MASK_ON) != MASK_ON) 
            break;
        
        item[i] = (void *)(((long)(array_val)) & ~MASK_ON);
        q->array[position * FACTOR] = 0;
        q->reader_ticket++;
    }
    q->lock = 0;
    return i;
}


/*******************************************************************/
/*               single producer single consumer                   */
/*******************************************************************/

#define DEFINE_QUEUE_SPSC(NAME,TYPE,SIZE)  \
typedef struct NAME##_t {\
    TYPE volatile * array;\
    volatile uint32_t tail __align ( CACHE_LINE ); /* input index*/\
    volatile uint32_t head __align ( CACHE_LINE ); /* output index*/\
} NAME##_t;\
\
\
INLINE void NAME##_init(NAME##_t * q) {\
    q->head = 0;\
    q->tail = 0;\
    q->array = (TYPE volatile *) _malloc(NEXT_POW2(SIZE+IS_PO2(SIZE)) * sizeof(TYPE));\
    uint32_t i;\
    for ( i = 0 ; i < NEXT_POW2(SIZE+IS_PO2(SIZE)); i++)\
        q->array[i]=0;\
}\
\
INLINE void NAME##_push(NAME##_t * q, TYPE item) {\
    uint32_t next_tail = ( q->tail + 1 ) & ( NEXT_POW2(SIZE+IS_PO2(SIZE)) - 1 );\
    q->array[q->tail] = item;\
    q->tail = next_tail;\
}\
\
INLINE int NAME##_pop(NAME##_t * q, TYPE* item) {\
    if ( q->head == q->tail )\
        return 0;\
    *item = q->array[q->head];\
    q->head = ( q->head + 1 ) & ( NEXT_POW2(SIZE+IS_PO2(SIZE)) - 1 );\
    return 1;\
}\
\
INLINE void NAME##_destroy(NAME##_t *q) {\
    free((void*) q->array);\
}

/*******************************************************************/
/*                      private queue                              */
/*******************************************************************/

#define DEFINE_QUEUE(NAME,TYPE,SIZE)  \
typedef struct NAME##_t {\
    TYPE * array;\
    uint32_t tail; /* input index*/\
    uint32_t head; /* output index*/\
} NAME##_t;\
\
\
INLINE void NAME##_init(NAME##_t * q) {\
    q->head = 0;\
    q->tail = 0;\
    q->array = (TYPE *) _malloc(NEXT_POW2(SIZE+IS_PO2(SIZE)) * sizeof(TYPE));\
    uint32_t i;\
    for ( i = 0 ; i < NEXT_POW2(SIZE+IS_PO2(SIZE)); i++)\
        q->array[i]=0;\
}\
\
INLINE void NAME##_push(NAME##_t * q, TYPE item) {\
    uint32_t next_tail = ( q->tail + 1 ) & ( NEXT_POW2(SIZE+IS_PO2(SIZE)) - 1 );\
    q->array[q->tail] = item;\
    q->tail = next_tail;\
}\
\
INLINE int NAME##_pop(NAME##_t * q, TYPE * item) {\
    if ( q->head == q->tail )\
        return 0;\
    *item = q->array[q->head];\
    q->head = ( q->head + 1 ) & ( NEXT_POW2(SIZE+IS_PO2(SIZE)) - 1 );\
    return 1;\
}\
\
INLINE int64_t NAME##_size (NAME##_t *q ) {\
    int64_t delta = ((int64_t)q->tail) - ((int64_t)q->head);\
    return ( delta >= (int64_t)0 ) ? delta : (int64_t) (NEXT_POW2(SIZE+IS_PO2(SIZE)) + delta);\
}\
\
INLINE void NAME##_destroy(NAME##_t *q) {\
    free(q->array);\
}
#endif
