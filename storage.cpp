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
	sscanf(sFile1, "bitcask_%u.w", &file_no1);
	sscanf(sFile2, "bitcask_%u.w", &file_no2);
	if(file_no1 > file_no2) {
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

//	int ret = 0;
	int ret = FindCurFile(CmpMaxWriteFile);
	if(ret != 0) {
		Log("FindCurFile fail", ret);
		return -1;
	}

	m_WriteFd = -1;

	return 0;
}


int Storage :: FindCurFile(Compare cmp)
{
	int file_len = 0;
	//attention no file in the dir
	char sFileName[256] = "bitcask_0.w";
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
		if(pDirent->d_name[file_len - 1] != 'w' ) {
//				|| pDirent->d_name[file_len - 1] != 'm') {
			continue;
		}

		if(cmp(pDirent->d_name, sFileName) > 0){
			strncpy(sFileName, pDirent->d_name, sizeof(sFileName));
		}
	}

	if(strncmp(sFileName, "bitcask_0.w", sizeof(sFileName)) == 0) {
		m_CurFilePos.file_no = 1;
		m_CurFilePos.file_pos = 0;
		if(pDir) closedir(pDir), pDir = NULL;
		return 0;
	}

	sscanf(sFileName, "bitcask_%u.w", &(m_CurFilePos.file_no));

	struct stat stat_buf;
	std::string sFilePath = m_FilePath + "/";
	sFilePath += sFileName;
	printf("sFilePath %s\n", sFilePath.c_str());
	int ret = stat(sFilePath.c_str(), &stat_buf);
	if(ret != 0) {
		Log("stat fail", ret);
		printf("stat err, errno %d, ret %d\n", errno, ret);
		if(pDir) closedir(pDir), pDir = NULL;
		return -1;
	}

	m_CurFilePos.file_pos = stat_buf.st_size;
	if(pDir) closedir(pDir), pDir = NULL;
	return 0;
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

	char sBuf[BITCASK_RECORD_BEGIN_LEN];
	ssize_t size = pread(fd, sBuf, sizeof(sBuf), file_pos);
	if(size == 0) {
		close(fd);
		return 0;
	} else if(size != sizeof(sBuf)) {
		Log("pread err, diff len", size);
		close(fd);
		return -1;
	}

	file_pos += size;

	int iRecordLen = BitCaskUtils::GetRecordLen(sBuf, sizeof(sBuf));
	printf("iRecordLen %d, size %d\n", iRecordLen, size);
	iRecordLen -= size;
	assert(iRecordLen > 0);
	char * pBuf = new char[iRecordLen];
	size = pread(fd, pBuf, iRecordLen, file_pos);
	close(fd);

	if(size == 0) {
		if(pBuf) delete []pBuf, pBuf = NULL;
		return 0;
	} else if(size != iRecordLen) {
		printf("pread err, diff len, size %d, recordlen %d\n", size, iRecordLen);
//		Log("pread err, diff len", size);
		if(pBuf) delete []pBuf, pBuf = NULL;
		return -1;
	}

	BitCaskUtils::Buf2Record(sBuf, sizeof(sBuf), pBuf, size, stRecord);

	if(pBuf) delete []pBuf, pBuf = NULL;
	assert(stRecord.crc == BITCAKS_CRC_NUM);
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
	stRecord.crc = 0;

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

int Storage :: Load2HashTable(HashTable & hashTable)
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
		if(pDirent->d_name[file_len - 1] != 'w' ) {
//				|| pDirent->d_name[file_len - 1] != 'm') {
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

	int file_no;
	sscanf(sFileName, "bitcask_%d.w", &file_no);

	ret = LoadBuf2HashTable(file_no, pBuf, stat_buf.st_size, hashTable);
	if(pBuf) delete []pBuf, pBuf = NULL;
	return ret;
}

int Storage :: LoadBuf2HashTable(int file_no, char * pBuf, int32_t len, HashTable & hashTable)
{
	std::string sKey;
	uint32_t iRecordLen = 0;
	char *pReadBuf = pBuf;
	int ret = 0;
	while((ret = BitCaskUtils::GetBufInfo(pReadBuf, len, iRecordLen, sKey)) == 0 || ret == 2) {
		if(ret == 0) {
			struct HashItem_t stItem;
			stItem.file_no = file_no;
			stItem.file_pos = pReadBuf - pBuf;
			hashTable[sKey] = stItem;
		}
		pReadBuf += iRecordLen;
		len -= iRecordLen;
//		printf("key %s, no %d, pos %d, recordlen %d\n", sKey.c_str(), file_no, pReadBuf-pBuf, iRecordLen);
	}

	return 0;
}

