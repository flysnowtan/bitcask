#include "bitcask.h"
#include "hashlock.h"
#include "bitcaskutils.h"

#include <functional>

BitCask :: BitCask()
{
}

BitCask :: ~BitCask()
{
	if(m_FileLock) {
		delete m_FileLock, m_FileLock = NULL;
	}
}

int BitCask :: Init()
{
	int ret = m_Storage.Init();
	if(ret != 0) {
		Log("m_Storage.Init err", ret);
		return -1;
	}

	ret = m_Storage.Load2HashTable(m_HashTable);
	if(ret != 0) {
		Log("m_Storage.Load2HashTable err", ret);
		return -1;
	}
	printf("after init hashtables size %d\n", m_HashTable.size());

	std::string sFileName = BITCASK_DEFAULT_PATH;
	sFileName += (std::string("/") + std::string("file_hash.lock"));
	m_FileLock = new clsFileLock(sFileName);
	if(m_FileLock == NULL) {
		Log("new err.", -1);
		return -1;
	}
	return 0;
}

int BitCask :: Get(const std::string & sKey, std::string & sVal)
{
	int ret = CheckKeyLen(sKey);
	if(ret != 0) {
		Log("CheckKeyLen fail", ret); 
	}

	struct Record_t stRecord;
	uint32_t iHash = BitCaskUtils::HashFunc(sKey);
	{
		clsHashLock hashLock(m_FileLock, iHash % BITCAKS_HASH_COUNT);
		hashLock.ReadLock();

		HashTable::iterator iter = m_HashTable.find(sKey);
		if(iter == m_HashTable.end()) {
			//		Log("not found", 1);
			return 1;
		}

		struct HashItem_t & hash_item = m_HashTable[sKey];
		//	printf("hash_item key %s, file_no %d, file_pos %d\n", sKey.c_str(), hash_item.file_no, hash_item.file_pos);
		ret = m_Storage.Get(hash_item.file_no, hash_item.file_pos, stRecord);
		if(ret != 0) {
			Log("m_Storage.Get fail", ret);
			return -1;
		}
	}

	if(stRecord.val != NULL) {
		sVal = std::string(stRecord.val, stRecord.val_sz);
		delete []stRecord.val, stRecord.val = NULL;
	}

	return 0;
}

int BitCask :: Set(const std::string &sKey, const std::string &sVal)
{
	return Add(sKey, sVal);
}

int BitCask :: Add(struct Record_t &stRecord)
{
	int32_t file_no = 0; 
	uint32_t file_pos = 0;
	uint32_t iHash = BitCaskUtils::HashFunc(std::string(stRecord.key, stRecord.key_sz));
	{
		clsHashLock hashLock(m_FileLock, iHash % BITCAKS_HASH_COUNT);
		hashLock.WriteLock();

		int ret = m_Storage.Add(stRecord, file_no, file_pos);
		if(ret != 0) {
			Log("m_Storage.Add fail", ret);
			return -1;
		}

		struct HashItem_t hash_item;
		hash_item.file_no = file_no;
		hash_item.file_pos = file_pos;

		std::string sKey(stRecord.key, stRecord.key_sz);
		m_HashTable[sKey] = hash_item;
	}
	return 0;
}

int BitCask :: Set(uint64_t uin, const std::string &sVal)
{
	char chKey[BITCASK_MAX_KEY_LEN];
	memcpy(chKey, &uin, sizeof(uint64_t));
	std::string sKey(chKey, sizeof(uint64_t));
	return Set(sKey, sVal);
}

int BitCask :: Delete(uint64_t uin)
{
	char chKey[BITCASK_MAX_KEY_LEN];
	memcpy(chKey, &uin, sizeof(uint64_t));
	std::string sKey(chKey, sizeof(uint64_t));
	return Delete(sKey);
}

int BitCask :: Get(uint64_t uin, std::string &sVal)
{
	char chKey[BITCASK_MAX_KEY_LEN];
	memcpy(chKey, &uin, sizeof(uint64_t));
	std::string sKey(chKey, sizeof(uint64_t));
	return Get(sKey, sVal);
}

int BitCask :: Add(uint64_t uin, const std::string & sVal)
{
	char chKey[BITCASK_MAX_KEY_LEN];
	memcpy(chKey, &uin, sizeof(uint64_t));
	std::string sKey(chKey, sizeof(uint64_t));
	return Add(sKey, sVal);
}

int BitCask :: Add(const std::string & sKey, const std::string & sVal)
{
	int ret = CheckKeyLen(sKey);
	if(ret != 0) {
		Log("CheckKeyLen fail", ret); 
	}

	struct Record_t stRecord;
	stRecord.key_sz = (uint8_t)sKey.size();
	stRecord.val_sz = sVal.size();
	memcpy(stRecord.key, sKey.c_str(), sKey.size());

	stRecord.val = new char[sVal.size()];
	memcpy(stRecord.val, sVal.c_str(), sVal.size());

	ret = Add(stRecord);

	if(stRecord.val) {
		delete []stRecord.val, stRecord.val = NULL;
	}

	return ret;
}

int BitCask :: Delete(const std::string & sKey)
{
	int ret = CheckKeyLen(sKey);
	if(ret != 0) {
		Log("CheckKeyLen fail", ret); 
	}
	uint32_t iHash = BitCaskUtils::HashFunc(sKey);
	{
		clsHashLock hashLock(m_FileLock, iHash % BITCAKS_HASH_COUNT);
		hashLock.WriteLock();

		HashTable::iterator iter = m_HashTable.find(sKey);
		if(iter == m_HashTable.end()) {
			//	Log("no such record", 1);
			return 1;
		}

		ret = m_Storage.Delete(sKey);
		if(ret != 0) {
			Log("m_Storage.Delete fail", ret);
			return -1;
		}

		m_HashTable.erase(iter);
	}
	return 0;
}

int BitCask :: CheckKeyLen(const std::string & sKey)
{
	if(sKey.size() > BITCASK_MAX_KEY_LEN || sKey.size() == 0) {
		return -1;
	}
	return 0;
}

int BitCask :: StartMerge()
{
	int ret = m_Storage.GenerateMergeFile(m_HashTable, m_FileLock);
	printf("after merge hashtables size %d\n", m_HashTable.size());
	return ret;
}

