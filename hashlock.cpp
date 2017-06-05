#include "hashlock.h"

clsHashLock::clsHashLock(clsFileLock *file_lock, uint32_t hash)
	:m_FileLock(file_lock), m_HashVal(hash)
{
}

clsHashLock::~clsHashLock()
{
	m_FileLock->unlock(m_HashVal, 1);
}

int clsHashLock::ReadLock()
{
	return m_FileLock->lockRBlock(m_HashVal, 1);
}

int clsHashLock::WriteLock()
{
	return m_FileLock->lockWBlock(m_HashVal, 1);
}
