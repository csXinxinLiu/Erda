#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/*  Cache line flush: 
    clflush can be replaced with clflushopt or clwb, if the CPU supports clflushopt or clwb.  
*/
#define asm_clflush(addr)                   \
({                              \
    __asm__ __volatile__ ("clflush %0" : : "m"(*addr)); \
})

/*  Memory fence:  
    mfence can be replaced with sfence, if the CPU supports sfence.
*/
#define asm_mfence()                \
({                      \
    __asm__ __volatile__ ("mfence":::"memory");    \
})


static int global_cpu_speed_mhz = 2400; //2399.941
static int global_write_latency_ns = 150;

void emulate_latency_ns(int ns);
void pflush(uint64_t *addr);

void init_pflush(int cpu_speed_mhz, int write_latency_ns);

