#pragma once

#include <stdint.h>
#include <stdio.h>
#include <map>

#define BITCAKS_CRC_NUM 1115
#define BITCASK_MAX_KEY_LEN 8
#define BITCASK_MAX_FILE_SIZE 1024*1024*2
#define BITCASK_RECORD_BEGIN_LEN (sizeof(uint8_t) + sizeof(uint32_t))
#define BITCASK_RECORD_LEN(key_sz, val_sz) (BITCASK_RECORD_BEGIN_LEN + key_sz + val_sz + sizeof(uint32_t))
#define BITCASK_DEFAULT_PATH "/home/alberttan/bitcask_path"
#define BITCASK_HINTREC_LEN(key_sz) (sizeof(uint8_t) + key_sz + sizeof(struct HashItem_t))
#define BITCASK_HINTREC_HAED_LEN (sizeof(uint8_t))

#define Log(str, ret)	do {printf("ERR: %s->%d, errmsg: %s, ret %d\n", __func__, __LINE__, str, ret); }while(0)
struct Record_t {
	uint8_t key_sz;
	uint32_t val_sz;
	char key[BITCASK_MAX_KEY_LEN];
	char *val;
	uint32_t crc;
};

struct HashItem_t {
	int32_t file_no;
	uint32_t file_pos;
};

struct FilePos {
	int32_t file_no;
	uint32_t file_pos;
};

struct HintRec_t {
	uint8_t key_sz;
	char key[BITCASK_MAX_KEY_LEN];
	struct HashItem_t hashItem;
};

typedef std::map<const std::string, struct HashItem_t> HashTable;
