#pragma once
#include <string>
#include <map>

#include "bitcask_global.h"
#include "storage.h"
		
class BitCask
{
	public:
		BitCask();
		~BitCask();
		int Init();
		int Get(const std::string & sKey, std::string &sVal);
		int Get(uint64_t uin, std::string &sVal);
		int Set(const std::string &sKey, const std::string &sVal);
		int Set(uint64_t uin, const std::string &sVal);
		int Add(const std::string & sKey, const std::string & sVal);
		int Add(uint64_t uin, const std::string & sVal);
		int Delete(const std::string & sKey);
		int Delete(uint64_t uin);
	private:
		int CheckKeyLen(const std::string & sKey);
		int Add(struct Record_t &stRecord);
	private:
		HashTable m_HashTable;
		Storage m_Storage;
};
