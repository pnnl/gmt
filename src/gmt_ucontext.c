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

#include "gmt/gmt_ucontext.h"

#if GMT_ENABLE_UCONTEXT

void gmt_setcontext(const ucontext_t *ucp) {
  __asm__ __volatile__(
#ifdef ENABLE_FPREGS
      /* Restore the floating-point context.  Not the registers, only the
         rest.  */
      "fldenv  " XSTR(oFPREGSMEM) " ( %0 )\n"
      "ldmxcsr " XSTR(oMXCSR)  " ( %0 )\n"
#endif
      "movq    " XSTR(oRSP) " ( %0 ), %%rsp\n"
      "movq    " XSTR(oRBX) " ( %0 ), %%rbx\n"
      "movq    " XSTR(oRBP) " ( %0 ), %%rbp\n"
      "movq    " XSTR(oR12) " ( %0 ), %%r12\n"
      "movq    " XSTR(oR13) " ( %0 ), %%r13\n"
      "movq    " XSTR(oR14) " ( %0 ), %%r14\n"
      "movq    " XSTR(oR15) " ( %0 ), %%r15\n"
      /* The following ret should return to the address set with gmt_getcontext.
         Therefore push the address on the stack.  */
      "movq    " XSTR(oRIP) " ( %0 ), %%rcx\n"
      "pushq   %%rcx\n"
      "movq    " XSTR(oRSI) " ( %0 ), %%rsi\n"
      "movq    " XSTR(oRDX) " ( %0 ), %%rdx\n"
      "movq    " XSTR(oRCX) " ( %0 ), %%rcx\n"
      "movq    " XSTR(oR8) " ( %0 ), %%r8\n"
      "movq    " XSTR(oR9) " ( %0 ), %%r9\n"
      /* Setup finally  %rdi.  */
      "movq    " XSTR(oRDI) " ( %0 ), %%rdi\n"
      "xorl    %%eax, %%eax\n"
      :
      :"r"(ucp));
  /* We don't need to put rcx in the clober list because this routine is
   * storing back the value in the ucp ucontext after the last use of rcx as
   * temporary register.
   */
}

void gmt_getcontext(ucontext_t *ucp) {
  __asm__ __volatile__(
        "movq    %%rbx, " XSTR(oRBX) " ( %0 )\n"
        "movq    %%rbp, " XSTR(oRBP) " ( %0 )\n"
        "movq    %%r12, " XSTR(oR12) " ( %0 )\n"
        "movq    %%r13, " XSTR(oR13) " ( %0 )\n"
        "movq    %%r14, " XSTR(oR14) " ( %0 )\n"
        "movq    %%r15, " XSTR(oR15) " ( %0 )\n"
        "movq    %%rdi, " XSTR(oRDI) " ( %0 )\n"
        "movq    %%rsi, " XSTR(oRSI) " ( %0 )\n"
        "movq    %%rdx, " XSTR(oRDX) " ( %0 )\n"
        "movq    %%rcx, " XSTR(oRCX) " ( %0 )\n"
        "movq    %%r8,  " XSTR(oR8) " ( %0 )\n"
        "movq    %%r9,  " XSTR(oR9) " ( %0 )\n"
        "movq    ( %%rsp ), %%rcx\n"
        "movq    %%rcx, " XSTR(oRIP) " ( %0 )\n"
        "leaq    8 ( %%rsp ), %%rcx\n"
        /* Exclude the return address.  */
        "movq    %%rcx, " XSTR(oRSP) " ( %0 )\n"
#ifdef ENABLE_FPREGS
        /* stack.  We use the __fpregs_mem block in the context.  Set the links
           up correctly.  */
        "leaq   " XSTR(oFPREGSMEM) " ( %0 ), %%rcx\n"
        "movq    %%rcx, " XSTR(oFPREGS) " ( %0 )\n"
        /* Save the floating-point environment.  */
        "fstenv " XSTR(oFPREGSMEM) "( %0 )\n"
        "stmxcsr " XSTR(oMXCSR) " ( %0 )\n"
#endif
        "xorl    %%eax, %%eax\n"
        :
        : "r"(ucp)
        : "rcx");
}

void gmt_swapcontext(ucontext_t *oucp, const ucontext_t *ucp) {
  __asm__ __volatile__(
      "mov    %%rbx, " XSTR(oRBX) " ( %0 )\n"
      "mov    %%rbp, " XSTR(oRBP) " ( %0 )\n"
      "mov    %%r12, " XSTR(oR12) " ( %0 )\n"
      "mov    %%r13, " XSTR(oR13) " ( %0 )\n"
      "mov    %%r14, " XSTR(oR14) " ( %0 )\n"
      "mov    %%r15, " XSTR(oR15) " ( %0 )\n"
      "mov    %%rdi, " XSTR(oRDI) " ( %0 )\n"
      "mov    %%rsi, " XSTR(oRSI) " ( %0 )\n"
      // "mov    %%rdx, " XSTR(oRDX) " ( %0 )\n"
      // "mov    %%rcx, " XSTR(oRCX) " ( %0 )\n"
      // "mov    %%r8,  " XSTR(oR8) " ( %0 )\n"
      // "mov    %%r9,  " XSTR(oR9) " ( %0 )\n"
      "mov    ( %%rsp ), %%rcx\n"
      "mov    %%rcx, " XSTR(oRIP) " ( %0 )\n"
      "lea    8 ( %%rsp ), %%rcx\n"
      "mov    %%rcx, " XSTR(oRSP) " ( %0 )\n"
      "fstenv  " XSTR(oFPREGSMEM) "( %0 )\n"
      "stmxcsr " XSTR(oMXCSR) " ( %0 )\n"
      "ldmxcsr " XSTR(oMXCSR) " ( %1 )\n"
      "fldenv  " XSTR(oFPREGSMEM) "( %1 )\n"
      "mov    " XSTR(oRSP) " ( %1 ), %%rsp\n"
      "mov    " XSTR(oRBX) " ( %1 ), %%rbx\n"
      "mov    " XSTR(oRBP) " ( %1 ), %%rbp\n"
      "mov    " XSTR(oR12) " ( %1 ), %%r12\n"
      "mov    " XSTR(oR13) " ( %1 ), %%r13\n"
      "mov    " XSTR(oR14) " ( %1 ), %%r14\n"
      "mov    " XSTR(oR15) " ( %1 ), %%r15\n"
      "mov    " XSTR(oRIP) " ( %1 ), %%rcx\n"
      "push   %%rcx\n"
      "mov    " XSTR(oRDI) " ( %1 ), %%rdi\n"
      "mov    " XSTR(oRSI) " ( %1 ), %%rsi\n"
      // "mov    " XSTR(oRDX) " ( %1 ), %%rdx\n"
      // "mov    " XSTR(oRCX) " ( %1 ), %%rcx\n"
      // "mov    " XSTR(oR8) " ( %1 ), %%r8\n"
      // "mov    " XSTR(oR9) " ( %1 ), %%r9\n"
      //"xorl    %%eax, %%eax\n"  //clear return value
      :
      : "r"(oucp), "r"(ucp));
}

void gmt_start_context() {
  __asm__ __volatile__(
      /* This removes the parameters passed to the function given to
         'gmt_makecontext' from the stack.  RBX contains the address
         on the stack pointer for the next context.  */
      "movq    %rbx, %rsp\n"
      "popq    %rdi\n"             /* This is the next context. */
      "testq   %rdi, %rdi\n"
      "je  2f\n"                   /* If it is zero exit. */
      "callq    gmt_setcontext\n"
      "2:\n"
      "movq    %rax,%rdi\n"
      "callq    exit\n"
      /* The 'exit' call should never return.  In case it does cause
         the process to terminate.  */
      "hlt\n");
}

void gmt_makecontext(ucontext_t *ucp, void (*func)(void), int argc, ...) {
  greg_t *sp;
  unsigned int idx_uc_link;
  va_list ap;
  int i;

  /* Generate room on stack for parameter if needed and uc_link. */
  sp = (greg_t *)(ucp->uc_stack.ss_sp + ucp->uc_stack.ss_size);

  sp -= (argc > 6 ? argc - 6 : 0) + 1;
  /* Align stack and make space for trampoline address. */
  sp = (greg_t *)((((uintptr_t)sp) & -16L) - 8);

  idx_uc_link = (argc > 6 ? argc - 6 : 0) + 1;

  /* Setup context ucp. */
  /* Address to jump to. */
  ucp->uc_mcontext.gregs[REG_RIP] = (uintptr_t)func;
  /* Setup rbx. */
  ucp->uc_mcontext.gregs[REG_RBX] = (uintptr_t)&sp[idx_uc_link];
  ucp->uc_mcontext.gregs[REG_RSP] = (uintptr_t)sp;

  /* Setup returning? stack. */
  sp[0] = (uintptr_t)&gmt_start_context;
  sp[idx_uc_link] = (uintptr_t)ucp->uc_link;

  va_start(ap, argc);
  /* Handle arguments.

     The standard says the parameters must all be int values.  This is
     an historic accident and would be done differently today.  For
     x86-64 all integer values are passed as 64-bit values and
     therefore extending the API to copy 64-bit values instead of
     32-bit ints makes sense.  It does not break existing
     functionality and it does not violate the standard which says
     that passing non-int values means undefined behavior.  */
  for (i = 0; i < argc; ++i) switch (i) {
      case 0:
        ucp->uc_mcontext.gregs[REG_RDI] = va_arg(ap, greg_t);
        break;
      case 1:
        ucp->uc_mcontext.gregs[REG_RSI] = va_arg(ap, greg_t);
        break;
      case 2:
        ucp->uc_mcontext.gregs[REG_RDX] = va_arg(ap, greg_t);
        break;
      case 3:
        ucp->uc_mcontext.gregs[REG_RCX] = va_arg(ap, greg_t);
        break;
      case 4:
        ucp->uc_mcontext.gregs[REG_R8] = va_arg(ap, greg_t);
        break;
      case 5:
        ucp->uc_mcontext.gregs[REG_R9] = va_arg(ap, greg_t);
        break;
      default:
        /* Put value on stack.  */
        sp[i - 5] = va_arg(ap, greg_t);
        break;
    }
  va_end(ap);
}
#endif

/* initialize context assigning a stack and a returning ctx when terminated */
void gmt_init_ctxt(ucontext_t *cntxt, void *stack, int stack_size,
                   ucontext_t *ret_cntxt) {
  gmt_getcontext(cntxt);
#if GMT_ENABLE_UCONTEXT
  cntxt->uc_flags = 0;
#else
  cntxt->__uc_flags = 0;
#endif
  cntxt->uc_link = ret_cntxt;
  cntxt->uc_stack.ss_sp = stack;
  cntxt->uc_stack.ss_size = stack_size;
  cntxt->uc_stack.ss_flags = 0;
}
