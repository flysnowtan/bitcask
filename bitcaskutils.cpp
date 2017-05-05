#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "bitcaskutils.h"
#include "bitcask_global.h"

int BitCaskUtils :: Buf2Record(char * pBuf, uint32_t len, struct Record_t & stRecord)
{
	if(pBuf == NULL || len < BITCASK_RECORD_BEGIN_LEN) {
		Log("param err", -1);
		return -1;
	}

	char *pPos = pBuf;
	stRecord.key_sz = *pPos;
	pPos += sizeof(uint8_t);

	memcpy(&(stRecord.val_sz), pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);

	memcpy(stRecord.key, pPos, stRecord.key_sz);
	pPos += stRecord.key_sz;

	if(stRecord.val_sz > 0) {
		stRecord.val = new char[stRecord.val_sz];
		memcpy(stRecord.val, pPos, stRecord.val_sz);
	} else {
		stRecord.val = NULL;
	}
	pPos += stRecord.val_sz;
	memcpy(&(stRecord.crc), pPos, sizeof(uint32_t));
	assert(pPos - pBuf == len);

	return 0;
}

int BitCaskUtils :: Record2Buf(struct Record_t & stRecord, char **ppBuf, uint32_t *len)
{
	uint32_t iLen = BITCASK_RECORD_LEN(stRecord.key_sz, stRecord.val_sz);
	char *pBuf = new char[iLen];
	*ppBuf = pBuf;
	
	*pBuf = stRecord.key_sz;
	pBuf += sizeof(uint8_t);

	memcpy(pBuf, &(stRecord.val_sz), sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	memcpy(pBuf, stRecord.key, stRecord.key_sz);
	pBuf += stRecord.key_sz;

	if(stRecord.val_sz > 0 && stRecord.val != NULL) {
		memcpy(pBuf, stRecord.val, stRecord.val_sz);
		pBuf += stRecord.val_sz;
	}

	memcpy(pBuf, &(stRecord.crc), sizeof(uint32_t));
	pBuf += sizeof(uint32_t);

	assert(pBuf - *ppBuf == iLen);
	*len = iLen;

	return 0;
}

int BitCaskUtils :: GetRecordLen(char *pBuf, uint32_t len)
{
	if(len == 0) {
		return 0;
	}
	int iKeyLen = 0;
	int iValLen = 0;

	char *pPos = pBuf;
	iKeyLen = *pPos;
	pPos += sizeof(uint8_t);

	memcpy(&iValLen, pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);

	assert(pPos - pBuf <= len);
	return BITCASK_RECORD_LEN(iKeyLen, iValLen);
}

int BitCaskUtils :: Buf2Record(char *pHeadBuf, uint32_t iHeadLen, char *pBuf, uint32_t len, struct Record_t & stRecord)
{
	char *pPos = pHeadBuf;
	stRecord.key_sz = *pPos;
	pPos += sizeof(uint8_t);

	memcpy(&(stRecord.val_sz), pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);
	assert(pPos - pHeadBuf == iHeadLen);

	pPos = pBuf;
	memcpy(stRecord.key, pPos, stRecord.key_sz);
	pPos += stRecord.key_sz;

	if(stRecord.val_sz > 0) {
		stRecord.val = new char[stRecord.val_sz];
		memcpy(stRecord.val, pPos, stRecord.val_sz);
	} else {
		stRecord.val = NULL;
	}

	pPos += stRecord.val_sz;

	memcpy(&(stRecord.crc), pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);

	assert(pPos - pBuf == len);

	return 0;
}

int BitCaskUtils :: GetBufInfo(char *pBuf, uint32_t len, uint32_t & recordLen, std::string & sKey)
{
	if(len == 0) {
		return 1;
	}

	int iKeyLen = 0;
	int iValLen = 0;

	char *pPos = pBuf;
	iKeyLen = *pPos;
	pPos += sizeof(uint8_t);

	memcpy(&iValLen, pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);

	recordLen = BITCASK_RECORD_LEN(iKeyLen, iValLen);
	sKey = std::string(pPos, iKeyLen);

	pPos = pPos + iKeyLen + iValLen;

	uint32_t crc = 0;
	memcpy(&crc, pPos, sizeof(uint32_t));
	pPos += sizeof(uint32_t);
	assert(pPos - pBuf <= len);
	assert(crc == BITCAKS_CRC_NUM);

	if(iValLen == 0) {
		return 2;
	}

	return 0;
}


