#ifndef DS_H 
#define DS_H 1
#define SEGMENTSIZE 8388608 //2^23-Byte (8MB)
#define HEADSIZE 1073741824 //2^30-Byte
#define HASHCapacity 200000
#define SEED 0x9c8d7e6f
#define MAX_SEARCH_RANGE 2
#define keysize 19
#define valuesize 16 //16,64,256,1024,4096 Bytes
#define xvs 0x24+0x4 //0x24,0x54,0x114,0x414,0x1014
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "crc32.h"
#include "murmurhash.h"

typedef bool uint1_t;


uint1_t getnewtag(uint64_t mark){
    return (mark>>63)&1;  
};

uint64_t setnewtag_newoffset(uint64_t mark, uint1_t nt, uint32_t offset){
    uint64_t offset_tmp = offset;
    if(nt) 
        return ((mark&0x7fffffffffffffff)|0x00000000fffffffe) & ((offset_tmp<<1)|0xffffffff00000000);
    else 
        return ((mark|0x8000000000000000)|0xffffffff00000000) & ((offset_tmp<<32)|0x80000000ffffffff);
};
uint64_t getnewoffset(uint64_t mark){
    if(getnewtag(mark))
        return (mark&0x7fffffff00000000)>>32;
    else
        return (mark&0x00000000fffffffe)>>1;
};
uint64_t getoldoffset(uint64_t mark){
    if(getnewtag(mark))
        return (mark&0x00000000fffffffe)>>1;
    else
        return (mark&0x7fffffff00000000)>>32;
};

#endif
