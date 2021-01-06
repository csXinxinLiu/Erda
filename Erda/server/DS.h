#ifndef DS_H 
#define DS_H 1
#define SEGMENTSIZE 8388608 //2^23-Byte (8MB)
#define HEADSIZE 1073741824 //2^30-Byte，
#define HASHCapacity 200000
#define SEED 0x9c8d7e6f
#define MAX_SEARCH_RANGE 2
#define keysize 19
#define valuesize 16 //16,64,256,1024,4096 Bytes
#define xvs (0x24+0x4) //0x24,0x54,0x114,0x414,0x1014 + 0x4 = the size of one object
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "crc32.h"
#include "murmurhash.h"
#include "pflush.h"  

typedef bool uint1_t;

typedef struct Object_{
    uint32_t checksum;
    char *data;
}* Object;

typedef struct Segment_{
    char data[HEADSIZE];
}* Segment;

typedef struct Head_{
    uint8_t head_id;
    uint32_t count;
    struct Segment_ * sroot;
}* Head;

typedef struct Bucket_{
    char key[keysize];
    uint8_t head_id;
    uint64_t mark;
}* Bucket;

typedef struct hashmap{
    size_t size;  //current size of the hash table 
    size_t capacity;  
    struct Bucket_ *array;
}* hmap;

typedef struct IDBucket_{
    size_t id;  
    uint1_t hnumber;
    uint8_t head_id;
    uint64_t mark;
}* IDBucket;


struct Bucket_ *HT;
struct Bucket_ *HTT;
Head HR[128];
int indx,nudx;

int hu[HASHCapacity];
int in[HASHCapacity];
int o=0;

uint1_t getnewtag(uint64_t mark){
    return (mark>>63)&1;  
};
uint1_t getreservetag(uint64_t mark){
    return mark&1;  
};


void setnewtag(uint64_t mark){
    mark = mark&0x7fffffffffffffff|1;
};

void setnewtag_newoffset(uint64_t mark, uint1_t nt, uint32_t offset){
    uint64_t offset_tmp = offset;
    if(nt){
        mark = (mark&0x7fffffff00000000) | ((offset_tmp<<1)&0x00000000ffffffff);
        //emulate_latency_ns(global_write_latency_ns); //++++++
    } 
        
    else{
        mark = (mark|0xffffffff00000000) & ((offset_tmp<<32)|0x80000000ffffffff);
        //emulate_latency_ns(global_write_latency_ns); //++++++
    } 
        
};
void client_setnewoffset(uint64_t mark, uint32_t offset){
    uint64_t offset_tmp = offset;
    mark = (mark|0xffffffff00000000) & ((offset_tmp<<32)|0x80000000ffffffff);
};
void server_setnewoffset(uint64_t mark, uint32_t offset){
    uint1_t nt = getnewtag(mark);
    uint64_t offset_tmp = offset;
    if(nt)
      mark = (mark|0x80000000fffffffe) & ((offset_tmp<<1)|0xffffffff00000000);
    else
      mark = ((mark<<31)|0x80000000fffffffe) & ((offset_tmp<<1)|0xffffffff00000000);
};

uint64_t getregion1offset(uint64_t mark){
    return (mark&0x7fffffff00000000)>>32;
};
uint64_t getregion2offset(uint64_t mark){
    return (mark&0x00000000fffffffe)>>1;
};

uint64_t getnewoffset(uint64_t mark){
    if(getnewtag(mark))
        return (mark&0x7fffffff00000000)>>32;
    else
        return (mark&0x00000000fffffffe)>>1;
};
uint64_t createsetmark(uint64_t offset){
    return (0x0&0x7fffffff00000000) | ((offset<<1)&0x00000000ffffffff);
};


uint8_t findHE(hmap H, hmap H2, char key[keysize]){
    size_t capacity = H->capacity; 
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;
    size_t limit = index + MAX_SEARCH_RANGE;
    //uint64_t offset;
    
    //very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        if(HT[index].key != NULL && strcmp(HT[index].key, key) == 0){
            //offset = (uintptr_t)((HR[HT[index].head_id]->count)*xvs);
            //setnewtag_newoffset(HT[index].mark, getnewtag(HT[index].mark), offset);
            indx = index;
            nudx = 0;
            return HT[index].head_id;
        }
        index++;
    }
    //to the second hashmap
    if(index == limit || index == capacity){
        size_t capacity2 = H2->capacity; 
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        size_t limit2 = index2 + MAX_SEARCH_RANGE;

        while(index2 < limit2 && index2 < capacity2){
            if(HTT[index2].key != NULL && strcmp(HTT[index2].key, key) == 0){
                //offset = (uintptr_t)((HR[HTT[index2].head_id]->count)*xvs);
                //setnewtag_newoffset(HTT[index2].mark, getnewtag(HTT[index2].mark), offset);
                indx = index2;
                nudx = 1;
                return HTT[index2].head_id;
            }
            index2++;
        }
    }
    return 128;
};

uint64_t findkey(hmap H, hmap H2, char key[keysize]){
    size_t capacity = H->capacity; 
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;
    size_t limit = index + MAX_SEARCH_RANGE;
    //uint64_t offset;
    
    //very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        if(HT[index].key != NULL && strcmp(HT[index].key, key) == 0){
            return HT[index].mark;
        }
        index++;
    }
    
    if(index == limit || index == capacity){
        size_t capacity2 = H2->capacity; 
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        size_t limit2 = index2 + MAX_SEARCH_RANGE;

        while(index2 < limit2 && index2 < capacity2){
            if(HTT[index2].key != NULL && strcmp(HTT[index2].key, key) == 0){
                return HTT[index2].mark;
            }
            index2++;
        }
    }
    return 1;
};

uint8_t setkey(hmap H, hmap H2, char key[keysize], uint8_t hid){

    if(H->size == H->capacity && H2->size == H2->capacity)
        return 128;


    size_t capacity = H->capacity;
    size_t capacity2 = H2->capacity;
    
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;

    //if the elem hashed to an empty slot, insert is successful!
    if(strlen(HT[index].key)==0){  
        (H->size)++;
        strcpy(HT[index].key,key);
        HT[index].head_id = hid;
        HT[index].mark = createsetmark((uint64_t)((HR[hid]->count)*xvs));
        //emulate_latency_ns(3*global_write_latency_ns);//++++++
        return 0;
    }

    //Otherwise, linearly probe for the next empty entry, but within the search bound, which is limited by MAX_SEARCH_RANGE
    size_t limit = index + MAX_SEARCH_RANGE; 
    //size_t capacity = H->capacity;
    size_t s_index = index;
    while(s_index < limit && s_index < capacity){
        if(strlen(HT[s_index].key)==0){ //empty slot found  
            (H->size)++;
            strcpy(HT[s_index].key,key);
            HT[s_index].head_id = hid;
            HT[s_index].mark = createsetmark((uint64_t)((HR[hid]->count)*xvs));
            //emulate_latency_ns(3*global_write_latency_ns);//++++++
            return 0;
        }       
        s_index++;
    }

    //both buckets are full
    if(s_index == limit || s_index == capacity){
        
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        //if the elem hashed to an empty slot, insert is successful!
        if(strlen(HTT[index2].key)==0){
            (H2->size)++;
            strcpy(HTT[index2].key,key);
            HTT[index2].head_id = hid;
            HTT[index2].mark = createsetmark((uint64_t)((HR[hid]->count)*xvs));
            //emulate_latency_ns(3*global_write_latency_ns);//++++++
            return 0;
        }

        //Otherwise, linearly probe for the next empty entry, but within the search bound, which is limited by MAX_SEARCH_RANGE
        limit = index2 + MAX_SEARCH_RANGE; 
        size_t s_index2 = index2;
        while(s_index2 < limit && s_index2 < capacity2){
            if(strlen(HTT[s_index2].key)==0){ //empty slot found
                (H2->size)++;
                strcpy(HTT[s_index2].key,key);
                HTT[s_index2].head_id = hid;
                HTT[s_index2].mark = createsetmark((uint64_t)((HR[hid]->count)*xvs));
                //emulate_latency_ns(3*global_write_latency_ns);//++++++
                return 0;
            }           
            s_index2++;
        }
    }
    return 128;
};

void setHE(hmap H, hmap H2, char key[keysize], uint32_t offset){
    
    size_t capacity = H->capacity; 
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;
    size_t limit = index + MAX_SEARCH_RANGE;
    
    //very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        
        if(HT[index].key != NULL && strcmp(HT[index].key, key) == 0){
            server_setnewoffset(HT[index].mark,offset);
            in[o]=index;
            hu[o]=0;
            o++;
        }
        index++;
    }
    
    if(index == limit || index == capacity){
       
        size_t capacity2 = H2->capacity; 
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        size_t limit2 = index2 + MAX_SEARCH_RANGE;

        while(index2 < limit2 && index2 < capacity2){
            
            if(HTT[index2].key != NULL && strcmp(HTT[index2].key, key) == 0){
                server_setnewoffset(HTT[index2].mark,offset);
                in[o]=index2;
                hu[o]=1;
                o++;
            }
            index2++;
        }
    }
};

void setHE2(hmap H, hmap H2, char key[keysize], uint32_t offset){
    
    size_t capacity = H->capacity; 
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;
    size_t limit = index + MAX_SEARCH_RANGE;
    
    //very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        
        if(HT[index].key != NULL && strcmp(HT[index].key, key) == 0){
            server_setnewoffset(HT[index].mark,offset);
        }
        index++;
    }
   
    if(index == limit || index == capacity){
        
        size_t capacity2 = H2->capacity; 
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        size_t limit2 = index2 + MAX_SEARCH_RANGE;

        while(index2 < limit2 && index2 < capacity2){
            
            if(HTT[index2].key != NULL && strcmp(HTT[index2].key, key) == 0){
                server_setnewoffset(HTT[index2].mark,offset);
            }
            index2++;
        }
    }
};


#endif
