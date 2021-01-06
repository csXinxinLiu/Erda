#ifndef DS_H 
#define DS_H 1
#define HASHCapacity 20000
#define SEED 0x9c8d7e6f
#define MAX_SEARCH_RANGE 2
#define keysize 19
#define valuesize 64
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "crc32.h"
#include "murmurhash.h"

typedef bool uint1_t;

typedef struct Bucket_{
	char key[keysize];
	uint64_t mark;
}* Bucket;

typedef struct hashmap{
	size_t size;  
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

/*IDBucket findHT(hmap H, hmap H2, char key[keysize]){
	//Bucket *array = H->array;
	size_t capacity = H->capacity; 
	size_t index = murmurhash(key, strlen(key), SEED) % capacity;
	size_t limit = index + MAX_SEARCH_RANGE;
    IDBucket idbucket = malloc(sizeof(struct IDBucket_));
    //printf("hash:%d\n",index);
    Bucket bucket = malloc(sizeof(struct Bucket_));
	//very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        bucket = &HT[index];
        if(bucket->key != NULL && strcmp(bucket->key, key) == 0){
            idbucket->head_id = bucket->head_id;
            idbucket->mark = bucket->mark;
            idbucket->id = index;
            idbucket->hnumber = 0;
            return idbucket;
        }
        index++;
    }
    //to the second hashmap
    if(index == limit || index == capacity){
    	//Bucket *array2 = H2->array;
	    size_t capacity2 = H2->capacity; 
	    size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
	    size_t limit2 = index2 + MAX_SEARCH_RANGE;

    	while(index2 < limit2 && index2 < capacity2){
            bucket = &HTT[index2];
            //printf("%d\n",index2);
            if(bucket->key != NULL && strcmp(bucket->key, key) == 0){
                //size_t x = bucket->head_id;
                idbucket->head_id = bucket->head_id;
                idbucket->mark = bucket->mark;
                idbucket->id = index2;
                idbucket->hnumber = 1;
                return idbucket;
            }
            index2++;
        }
    }
    return NULL;
};
*/

Bucket findHE(hmap H, hmap H2, char key[keysize]){
	//Bucket *array = H->array;
	size_t capacity = H->capacity; 
	size_t index = murmurhash(key, strlen(key), SEED) % capacity;
	size_t limit = index + MAX_SEARCH_RANGE;
    //IDBucket idbucket;
    Bucket bucket;
	//very convenient get: any inserted element is always found at its hashed_index or in the next NSIZE-1 indices. Makes get(H,key) O(1).
    while(index < limit && index < capacity){
        bucket = &HT[index];
        if(bucket->key != NULL && strcmp(bucket->key, key) == 0){
            return bucket;
        }
        index++;
    }
    //to the second hashmap
    if(index == limit || index == capacity){
    	//Bucket *array2 = H2->array;
	    size_t capacity2 = H2->capacity; 
	    size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
	    size_t limit2 = index2 + MAX_SEARCH_RANGE;

    	while(index2 < limit2 && index2 < capacity2){
            bucket = &HTT[index2];
            if(bucket->key != NULL && strcmp(bucket->key, key) == 0){
                return bucket;
            }
            index2++;
        }
    }
    return NULL;
};

/*IDBucket setkey(hmap H, hmap H2, char key[keysize], uint8_t hid){
	if(H->size == H->capacity && H2->size == H2->capacity)
		return NULL;

	Bucket bucket = malloc(sizeof(struct Bucket_));
	strcpy(bucket->key,key);
	bucket->mark = 0x0;
	bucket->head_id = hid;

    size_t capacity = H->capacity;
    size_t capacity2 = H2->capacity;
	
    size_t index = murmurhash(key, strlen(key), SEED) % capacity;
	//Bucket *array = H->array;
	//Bucket *array2 = H2->array;

	IDBucket idbucket = malloc(sizeof(struct IDBucket_));
    Bucket b;
	b = &HT[index];
    //if(b) printf("%s,%d,%x\n",b->key,b->head_id,b->mark);
                
    //if the elem hashed to an empty slot, insert is successful!
	if(strlen(b->key)==0){	
        (H->size)++;
        strcpy(HT[index].key,key);
        HT[index].head_id = hid;
        idbucket->head_id = bucket->head_id;
        idbucket->mark = bucket->mark;
        idbucket->id = index;
        idbucket->hnumber = 0;
        return idbucket;
    }

    //Otherwise, linearly probe for the next empty entry, but within the search bound, which is limited by MAX_SEARCH_RANGE
    size_t limit = index + MAX_SEARCH_RANGE; 
    //size_t capacity = H->capacity;
    size_t s_index = index;
    while(s_index < limit && s_index < capacity){
        b = &HT[s_index];
        if(strlen(b->key)==0){ //empty slot found  
        	(H->size)++;
        	strcpy(HT[s_index].key,key);
            HT[s_index].head_id = hid;
            idbucket->head_id = bucket->head_id;
            idbucket->mark = bucket->mark;
            idbucket->id = s_index;
            idbucket->hnumber = 0;
            return idbucket;
        }       
        s_index++;
    }

    //both are full
    if(s_index == limit || s_index == capacity){
        
        size_t index2 = murmurhash(key, strlen(key), SEED) % capacity2;
        b = &HTT[index2];
        //if the elem hashed to an empty slot, insert is successful!
	    if(strlen(b->key)==0){
		    //array2[index2] = bucket;
            (H2->size)++;
            strcpy(HTT[index2].key,key);
            HTT[index2].head_id = hid;
            idbucket->head_id = bucket->head_id;
            idbucket->mark = bucket->mark;
            idbucket->id = index2;
            idbucket->hnumber = 1;
            return idbucket;
        }

        //Otherwise, linearly probe for the next empty entry, but within the search bound, which is limited by MAX_SEARCH_RANGE
        limit = index2 + MAX_SEARCH_RANGE; 
        size_t s_index2 = index2;
        while(s_index2 < limit && s_index2 < capacity2){
            b = &HTT[s_index2];
            if(strlen(b->key)==0){ //empty slot found
                (H2->size)++;
                strcpy(HTT[s_index2].key,key);
                HTT[s_index2].head_id = hid;
                idbucket->head_id = bucket->head_id;
                idbucket->mark = bucket->mark;
                idbucket->id =s_index2;
                idbucket->hnumber = 1;
                return idbucket;
            }           
            s_index2++;
        }
    }
    //free(bucket);
    return NULL;
};
*/

#endif
