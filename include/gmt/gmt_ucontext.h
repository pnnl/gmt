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

#ifndef __GMT_UCONTEXT__
#define __GMT_UCONTEXT__

#include "gmt/config.h"

#if  !GMT_ENABLE_UCONTEXT
/* use libc ucontext primitives */
#include <signal.h>
#include <stdio.h>
#include <signal.h>
#include <ucontext.h>
#define  gmt_swapcontext(oucp,ucp)      swapcontext(oucp,ucp)
#define  gmt_makecontext(...)           makecontext(__VA_ARGS__)
#define  gmt_getcontext(ucp)            getcontext(ucp)

void gmt_init_ctxt(ucontext_t * cntxt, void *stack,
                   int stack_size, ucontext_t * ret_cntxt);

#else

#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>

#ifndef __USE_GNU
#define __USE_GNU
#endif
/* Get machine dependent definition of data structures.  */
#include <sys/ucontext.h>

#define ENABLE_FPREGS

#define oGREGS      40
#define oFPREGS     224
#define oSIGMASK    296
#define oFPREGSMEM  424
#define oMXCSR      448

#define oRBP        120
#define oRSP        160
#define oRBX        128
#define oR8         40
#define oR9         48
#define oR10        56
#define oR11        64
#define oR12        72
#define oR13        80
#define oR14        88
#define oR15        96
#define oRDI        104
#define oRSI        112
#define oRDX        136
#define oRAX        144
#define oRCX        152
#define oRIP        168
#define oEFL        176

#define XSTR(x) STR(x)
#define STR(x) #x

#if defined(__cplusplus)
extern "C" {
#endif
void gmt_setcontext(const ucontext_t * ucp) __attribute__ ((noinline));
void gmt_getcontext(ucontext_t * ucp) __attribute__ ((noinline));
void gmt_start_context() __attribute__ ((noinline));
void gmt_init_ctxt(ucontext_t * cntxt, void *stack, int stack_size, ucontext_t * ret_cntxt);
void gmt_swapcontext(ucontext_t * oucp, const ucontext_t * ucp) __attribute__ ((noinline));
void gmt_makecontext(ucontext_t * ucp, void (*func) (void), int argc, ...) __attribute__ ((noinline));
#if defined(__cplusplus)
}
#endif

#endif
#endif
