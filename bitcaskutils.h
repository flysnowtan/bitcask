#pragma once
#include <stdint.h>
#include <string>

class BitCaskUtils
{
	public:
		static int Buf2Record(char * pBuf, uint32_t len, struct Record_t & stRecord);
		static int Buf2Record(char *pHeadBuf, uint32_t iHeadLen, char *pBuf, uint32_t len, struct Record_t & stRecord);
		static int Record2Buf(struct Record_t & stRecord, char **ppBuf, uint32_t *len);

		static int GetRecordLen(char *pBuf, uint32_t len);
		static int GetBufInfo(char *pBuf, uint32_t len, uint32_t & recordLen, std::string & sKey);

		static int Write2File(int fd, char *pBuf, uint32_t len);
		static int HintRec2Buf(struct HintRec_t hintRec, char **ppBuf, uint32_t *len);
};
