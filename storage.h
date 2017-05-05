#pragma once
#include <stdint.h>
#include <string>
#include "bitcask_global.h"

class Storage
{
	typedef int (*Compare )(const char *sFile1, const char *sFile2);
	public:
		Storage();
		~Storage();
		int Init(const char *sPath = NULL);
		int Get(int32_t file_no, uint32_t file_pos, struct Record_t & stRecord);
		int Add(struct Record_t & stRecord, int32_t & file_no, uint32_t & file_pos);
		int Delete(const std::string & sKey);

		int Load2HashTable(HashTable & hashTable);
	private:
		int Open(uint32_t file_no, int iFlag, char suffix);
		int FindCurFile(Compare cmp);
		int LoadFile2HashTable(const char * sFileName, HashTable & hashTable);
		int LoadBuf2HashTable(int file_no, char * pBuf, int32_t len, HashTable & hashTable);
	private:
		std::string m_FilePath;
		struct FilePos m_CurFilePos;
		int m_WriteFd;
};
