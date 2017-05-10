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
		int GenerateMergeFile(HashTable & hashTable);
	private:
		int Open(uint32_t file_no, int iFlag, char suffix);
		int FindFile(Compare cmp);
		int LoadFile2HashTable(const char * sFileName, HashTable & hashTable);
		int LoadBuf2HashTable(const char * sFileName, char * pBuf, int32_t len, HashTable & hashTable);
		int LoadHintFile2HashTable(uint32_t file_no, char suffix, HashTable & hashTable);
		int LoadWriteOrMergeFile2HashTable(uint32_t file_no, char suffix, HashTable & hashTable);
		int AddRecord2MergeFile();
		int MergeFile(int file_no, char suffix, int & merge_file_no, HashTable & hashTable);
		int GetOneRecord(int fd, uint32_t file_pos, struct Record_t & stRecord);
		int GetOneRecord(int fd, uint32_t file_pos, struct HintRec_t & stRecord);
	private:
		std::string m_FilePath;
		struct FilePos m_CurFilePos;
		int m_WriteFd;
};
