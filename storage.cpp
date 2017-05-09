#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include "storage.h"
#include "bitcaskutils.h"

static int CmpMaxWriteFile(const char * sFile1, const char * sFile2)
{
	uint32_t file_no1, file_no2;
	char suffix1, suffix2;
	sscanf(sFile1, "bitcask_%u.%c", &file_no1, &suffix1);
	sscanf(sFile2, "bitcask_%u.%c", &file_no2, &suffix2);
	if(suffix1 != 'w' || (sFile2[0] != '\0' && suffix2 != 'w')) {
		return -1;
	}

	if(*sFile2 == '\0') {
		return 1;
	}

	if(file_no1 > file_no2) {
		return 1;
	}
	return 0;
}

static int CmpMaxMergeFile(const char * sFile1, const char * sFile2)
{
	uint32_t file_no1, file_no2;
	char suffix1, suffix2;
	sscanf(sFile1, "bitcask_%u.%c", &file_no1, &suffix1);
	sscanf(sFile2, "bitcask_%u.%c", &file_no2, &suffix2);
	if(suffix1 != 'm' || (sFile2[0] != '\0' && suffix2 != 'm')) {
		return -1;
	}

	if(*sFile2 == '\0') {
		return 1;
	}

	if(file_no1 > file_no2) {
		return 1;
	}
	return 0;
}

static int CmpMinWriteFile(const char * sFile1, const char * sFile2)
{
	uint32_t file_no1, file_no2;
	char suffix1, suffix2;
	sscanf(sFile1, "bitcask_%u.%c", &file_no1, &suffix1);
	sscanf(sFile2, "bitcask_%u.%c", &file_no2, &suffix2);
	if(suffix1 != 'w' || (sFile2[0] != '\0' && suffix2 != 'w')) {
		return -1;
	}

	if(*sFile2 == '\0') {
		return 1;
	}

	if(file_no1 < file_no2) {
		return 1;
	}

	return 0;
}

static int CmpMinMergeFile(const char * sFile1, const char * sFile2)
{
	uint32_t file_no1, file_no2;
	char suffix1, suffix2;
	sscanf(sFile1, "bitcask_%u.%c", &file_no1, &suffix1);
	sscanf(sFile2, "bitcask_%u.%c", &file_no2, &suffix2);
	if(suffix1 != 'm' || (sFile2[0] != '\0' && suffix2 != 'm')) {
		return -1;
	}

	if(*sFile2 == '\0') {
		return 1;
	}

	if(file_no1 < file_no2) {
		return 1;
	}
	return 0;
}

Storage :: Storage()
{
}

Storage :: ~Storage()
{
	if(m_WriteFd != -1) {
		close(m_WriteFd);
	}
}

int Storage :: Init(const char * sPath)
{
	if(sPath == NULL) {
		m_FilePath = BITCASK_DEFAULT_PATH;
	} else {
		m_FilePath = sPath;
	}

	m_WriteFd = -1;

	int ret = 0;
	int file_no = FindFile(CmpMaxWriteFile);
	if(file_no < 0) {
		Log("FindFile fail", file_no);
		return -1;
	}

	if(file_no == 0) {
		m_CurFilePos.file_no = 1;
		m_CurFilePos.file_pos = 0;
		printf("cur write file_no %d, file_pos %d\n", m_CurFilePos.file_no, m_CurFilePos.file_pos);
		return 0;
	}

	int fd = Open(file_no, O_RDONLY, 'w');
	if(fd < 0) {
		Log("Open err", fd);
		return -1;
	}

	struct stat file_stat;
	ret = fstat(fd, &file_stat);
	close(fd);
	if(ret < 0) {
		Log("fstat err", ret);
		return -1;
	}

	m_CurFilePos.file_no = file_no;
	m_CurFilePos.file_pos = file_stat.st_size;
	printf("cur write file_no %d, file_pos %d\n", m_CurFilePos.file_no, m_CurFilePos.file_pos);

	return 0;
}

int Storage :: GetOneRecord(int fd, uint32_t file_pos, struct Record_t & stRecord)
{
	char sBuf[BITCASK_RECORD_BEGIN_LEN];
	ssize_t size = pread(fd, sBuf, sizeof(sBuf), file_pos);
	if(size == 0) {
		return 0;
	} else if(size != sizeof(sBuf)) {
		Log("pread err, diff len", size);
		return -1;
	}

	file_pos += size;

	int iRecordLen = BitCaskUtils::GetRecordLen(sBuf, sizeof(sBuf));
//	printf("iRecordLen %d, size %d\n", iRecordLen, size);
	iRecordLen -= size;
	assert(iRecordLen > 0);
	char * pBuf = new char[iRecordLen];
	size = pread(fd, pBuf, iRecordLen, file_pos);

	if(size == 0) {
		if(pBuf) delete []pBuf, pBuf = NULL;
		return 0;
	} else if(size != iRecordLen) {
		printf("pread err, diff len, size %d, recordlen %d\n", size, iRecordLen);
		if(pBuf) delete []pBuf, pBuf = NULL;
		return -1;
	}

	BitCaskUtils::Buf2Record(sBuf, sizeof(sBuf), pBuf, size, stRecord);

	if(pBuf) delete []pBuf, pBuf = NULL;
	
//	printf("val %s, crc %d, valempty %d\n", std::string(stRecord.val, stRecord.val_sz).c_str(), stRecord.crc, stRecord.val == NULL);
	assert(stRecord.crc == BITCAKS_CRC_NUM);

	return file_pos + iRecordLen;
}

int Storage :: FindFile(Compare cmp)
{
	int file_len = 0;
	//attention no file in the dir
	char sFileName[256] = {0};
	DIR * pDir = opendir(m_FilePath.c_str());
	if(pDir == NULL) {
		Log("opendir fail", -1);
		return -1;
	}

	struct dirent * pDirent = NULL;
	while((pDirent = readdir(pDir)) != NULL) {
		if(strncmp(pDirent->d_name, "..", sizeof(sFileName)) == 0
				|| strncmp(pDirent->d_name, ".", sizeof(sFileName)) == 0) {
			continue;
		}
		file_len = strlen(pDirent->d_name);
		if(pDirent->d_name[file_len - 1] != 'w' 
				&& pDirent->d_name[file_len - 1] != 'm') {
			continue;
		}

		if(cmp(pDirent->d_name, sFileName) > 0){
			strncpy(sFileName, pDirent->d_name, sizeof(sFileName));
		}
	}
	if(pDir) closedir(pDir), pDir = NULL;

	if(sFileName[0] == '\0') {
		return 0;
	}

	uint32_t file_no;
	char suffix;
	printf("file name %s\n", sFileName);
	sscanf(sFileName, "bitcask_%u.%c", &file_no, &suffix);
	return file_no;
}

int Storage :: Get(int32_t file_no, uint32_t file_pos, struct Record_t & stRecord)
{
	uint32_t real_file_no = file_no;
	char cSuffix = 'w';
	if(file_no < 0) {
		cSuffix = 'm';
		real_file_no = -file_no;
	}

	int fd = Open(real_file_no, O_RDONLY, cSuffix);
	if(fd < 0) {
		Log("open fail", fd);
		return -1;
	}

	file_pos = GetOneRecord(fd, file_pos, stRecord);
	close(fd);
	if(file_pos < 0) {
		Log("GetOneRecord err", file_pos);
		return -1;
	}

	return 0;
}

int Storage :: Add(struct Record_t & stRecord, int32_t & file_no, uint32_t & file_pos)
{
	char *pBuf = NULL;
	uint32_t len = 0;
	stRecord.crc = BITCAKS_CRC_NUM;
	BitCaskUtils::Record2Buf(stRecord, &pBuf, &len);
	if(m_CurFilePos.file_pos + len > BITCASK_MAX_FILE_SIZE) {
		m_CurFilePos.file_no++;
		m_CurFilePos.file_pos = 0;
		close(m_WriteFd);
		m_WriteFd = -1;
	}

	if(m_WriteFd == -1) {
		m_WriteFd = Open(m_CurFilePos.file_no, O_CREAT | O_WRONLY | O_APPEND, 'w');
		if(m_WriteFd < 0) {
			if(pBuf) delete []pBuf, pBuf = NULL;
			Log("Open fail", m_WriteFd);
			return -1;
		}
	}

	int size = 0;
	int iLeft = len;
	char *pWritePos = pBuf;
	while(iLeft > 0) {
		size = write(m_WriteFd, pWritePos, iLeft);
		if(size < 0) {
			Log("write fail", size);
			if(pBuf) delete []pBuf, pBuf = NULL;
			return -1;
		} else {
			iLeft -= size;
			pWritePos += size;
		}
	}

	if(pBuf) delete []pBuf, pBuf = NULL;
	file_no = m_CurFilePos.file_no;
	file_pos = m_CurFilePos.file_pos;
	m_CurFilePos.file_pos += len;

	return 0;
}

int Storage :: Delete(const std::string & sKey)
{
	struct Record_t stRecord;
	stRecord.key_sz = sKey.size();
	stRecord.val_sz = 0;
	assert(sKey.size() <= BITCASK_MAX_KEY_LEN);

	memcpy(stRecord.key, sKey.c_str(), sKey.size());
	stRecord.val = NULL;
	stRecord.crc = BITCAKS_CRC_NUM;

	int32_t file_no = -1;
	uint32_t file_pos = 0;
	return Add(stRecord, file_no, file_pos);
}

int Storage :: Open(uint32_t file_no, int iFlag, char suffix)
{
	int fd = -1;
	char file_name[256];
	//file_no start from 1
	if(file_no == 0) {
		Log("wrong file_no: 0", 0);
		return -1;
	}

	snprintf(file_name, sizeof(file_name), "%s/bitcask_%u.%c", m_FilePath.c_str(), file_no, suffix);
	fd = open(file_name, iFlag, 0666);
	if(fd < 0) {
		printf("open file fail, fd %d, errno %d\n", fd, errno);
//		Log("open file fail", fd);
		return -1;
	}

	return fd;
}

int Storage :: LoadFile2HashTable(char suffix, HashTable & hashTable)
{
	int file_len = 0;
	//attention no file in the dir
	DIR * pDir = opendir(m_FilePath.c_str());
	if(pDir == NULL) {
		Log("opendir fail", -1);
		return -1;
	}

	struct dirent * pDirent = NULL;
	while((pDirent = readdir(pDir)) != NULL) {
		if(strncmp(pDirent->d_name, "..", 256) == 0
				|| strncmp(pDirent->d_name, ".", 256) == 0) {
			continue;
		}
		file_len = strlen(pDirent->d_name);
		if(pDirent->d_name[file_len - 1] != suffix) {
			continue;
		}

		int ret = LoadFile2HashTable(pDirent->d_name, hashTable);
		if(ret != 0) {
			Log("LoadFile2HashTable fail", ret);
			if(pDir) closedir(pDir), pDir = NULL;
			return -1;
		}
	}

	if(pDir) closedir(pDir), pDir = NULL;
	return 0;
}

int Storage :: Load2HashTable(HashTable & hashTable)
{
	int ret = LoadFile2HashTable('m', hashTable);
	if(ret != 0) {
		Log("LoadFile2HashTable fail", ret);
		return -1;
	}

	ret = LoadFile2HashTable('w', hashTable);
	if(ret != 0) {
		Log("LoadFile2HashTable fail", ret);
		return -1;
	}

	return 0;
}

int Storage :: LoadFile2HashTable(const char * sFileName, HashTable & hashTable)
{
	if(sFileName == NULL) {
		return 0;
	}
	struct stat stat_buf;
	std::string sFilePath = m_FilePath + "/";
	sFilePath += sFileName;
	int ret = stat(sFilePath.c_str(), &stat_buf);
	if(ret != 0) {
		Log("stat fail", ret);
		return -1;
	}
	//if size is too large, err occur
	//todo: new once one record
	char * pBuf = new char[stat_buf.st_size];
	int fd = open(sFilePath.c_str(), O_RDONLY, 0666);
	if(fd < 0) {
		Log("open file fail", fd);
		return -1;
	}

	int size = 0;
	int iLeft = stat_buf.st_size;
	char *pReadPos = pBuf;
	while(iLeft > 0) {
		size = read(fd, pReadPos, iLeft);
		if(size < 0) {
			Log("read err", size);
			if(pBuf) delete []pBuf, pBuf = NULL;
			return -1;
		} else {
			iLeft -= size;
			pReadPos += size;
		}
	}


	ret = LoadBuf2HashTable(sFileName, pBuf, stat_buf.st_size, hashTable);
	if(pBuf) delete []pBuf, pBuf = NULL;
	return ret;
}

int Storage :: LoadBuf2HashTable(const char *sFileName, char * pBuf, int32_t len, HashTable & hashTable)
{
	std::string sKey;
	uint32_t iRecordLen = 0;
	char *pReadBuf = pBuf;
	int ret = 0;

	int file_no;
	char suffix;
	sscanf(sFileName, "bitcask_%d.%c", &file_no, &suffix);
	if(suffix == 'm') {
		file_no = -file_no;
	}

	while((ret = BitCaskUtils::GetBufInfo(pReadBuf, len, iRecordLen, sKey)) == 0 || ret == 2) {
		if(ret == 0) {
			struct HashItem_t stItem;
			stItem.file_no = file_no;
			stItem.file_pos = pReadBuf - pBuf;
			hashTable[sKey] = stItem;
		} else if(ret == 2) {
			hashTable.erase(sKey);
		}
		
		pReadBuf += iRecordLen;
		len -= iRecordLen;
//		printf("key %s, no %d, pos %d, recordlen %d\n", sKey.c_str(), file_no, pReadBuf-pBuf, iRecordLen);
	}

	return 0;
}

int Storage :: GenerateMergeFile(HashTable & hashTable)
{
	//merge .m file first
	int min_merge_file_no = FindFile(CmpMinMergeFile);
	int max_merge_file_no = FindFile(CmpMaxMergeFile);
	int ret = 0;
	if(min_merge_file_no > 0) {
		for(int i = min_merge_file_no; i < max_merge_file_no; i++) {
			ret = MergeFile(i, 'm', hashTable);
			if(ret != 0) {
				Log("MergeFile err", ret);
				continue;
			}
		}
	}

	//then merge .w file
	int min_write_file_no = FindFile(CmpMinWriteFile);
	int max_write_file_no = FindFile(CmpMaxWriteFile);
	if(min_write_file_no > 0) {
		for(int i = min_write_file_no; i < max_write_file_no; i++) {
			ret = MergeFile(i, 'w', hashTable);
			if(ret != 0) {
				Log("MergeFile err", ret);
				continue;
			}
		}
	}

	return 0;
}

int Storage :: MergeFile(int file_no, char suffix, HashTable & hashTable)
{
	int max_merge_file_no = FindFile(CmpMaxMergeFile);
	if(max_merge_file_no < 0) {
		Log("FindFile err", max_merge_file_no);
		return -1;
	}

	int fd = Open(file_no, O_RDONLY, suffix);
	if(fd < 0) {
		Log("Open err", fd);
		return -1;
	}
	
	char file_name[256];
	int next_file_pos = 0;
	int ret = 0;
	struct Record_t stRecord;
	int32_t merge_file_no = (max_merge_file_no == 0 ? 1 : max_merge_file_no);
	uint32_t merge_file_pos = 0;
	int merge_fd = -1;
	if(max_merge_file_no > 0) {
		snprintf(file_name, sizeof(file_name), "%s/bitcask_%u.m", m_FilePath.c_str(), merge_file_no);
		struct stat stat_buf;
		ret = stat(file_name, &stat_buf);
		if(ret != 0) {
			Log("stat fail", ret);
			return -1;
		}
		merge_file_pos = stat_buf.st_size;
	}

	while((next_file_pos = GetOneRecord(fd, next_file_pos, stRecord)) > 0) {
		//need release buf in stRecord

		HashTable::iterator iter = hashTable.find(std::string(stRecord.key, stRecord.key_sz));
		if(iter == hashTable.end()) {
			if(stRecord.val) delete []stRecord.val, stRecord.val = NULL;
			continue;
		}

		if((suffix == 'w' && iter->second.file_no != file_no)
				|| (suffix == 'm' && iter->second.file_no != -file_no)
				|| (iter->second.file_pos != next_file_pos - BITCASK_RECORD_LEN(stRecord.key_sz, stRecord.val_sz))) {
			if(stRecord.val) delete []stRecord.val, stRecord.val = NULL;
			continue;
		}

		char *pBuf = NULL;
		uint32_t len = 0;
		BitCaskUtils::Record2Buf(stRecord, &pBuf, &len);
		if(stRecord.val) delete []stRecord.val, stRecord.val = NULL;

		if(merge_file_pos + len > BITCASK_MAX_FILE_SIZE) {
			merge_file_no++;
			merge_file_pos = 0;
			if(merge_fd > 0) {
				close(merge_fd);
			}
			merge_fd = -1;
		}

		if(merge_fd == -1) {
			merge_fd = Open(merge_file_no, O_CREAT | O_WRONLY | O_APPEND, 'm');
			if(merge_fd < 0) {
				if(pBuf) delete []pBuf, pBuf = NULL;
				Log("Open fail", merge_fd);
				close(fd);
				return -1;
			}
		}

		struct HashItem_t hashItem;
		hashItem.file_no = -merge_file_no;
		hashItem.file_pos = merge_file_pos;

		int size = 0;
		int iLeft = len;
		char *pWritePos = pBuf;
		while(iLeft > 0) {
			size = write(merge_fd, pWritePos, iLeft);
			if(size < 0) {
				Log("write fail", size);
				if(pBuf) delete []pBuf, pBuf = NULL;
				close(fd);
				close(merge_fd);
				return -1;
			} else {
				iLeft -= size;
				pWritePos += size;
			}
		}

		if(pBuf) delete []pBuf, pBuf = NULL;
		merge_file_pos += len;
		hashTable[std::string(stRecord.key, stRecord.key_sz)] = hashItem;
//		printf("hashitem for merge, file_no %d, file_pos %d\n", hashItem.file_no, hashItem.file_pos);
	}

	close(fd);
	if(merge_fd > 0) {
		close(merge_fd);
	}
	snprintf(file_name, sizeof(file_name), "%s/bitcask_%u.%c", m_FilePath.c_str(), file_no, suffix);
	printf("remove file_name %s, merge file %d\n", file_name, merge_file_no);
	remove(file_name);

	return 0;
}

