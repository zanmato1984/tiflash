#pragma once

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Encryption/KeyManager.h>
#include <common/likely.h>
#include <Poco/Path.h>

namespace DB
{
class DataKeyManager : public KeyManager
{
public:
    DataKeyManager(TiFlashServer * tiflash_instance_wrap_) : tiflash_instance_wrap{tiflash_instance_wrap_} {}

    ~DataKeyManager() = default;

    FileEncryptionInfo getFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->getFile(Poco::Path(fname).toString());
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException(
                "Get encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
        return r;
    }

    FileEncryptionInfo newFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->newFile(Poco::Path(fname).toString());
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException(
                "Create encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
        return r;
    }

    void deleteFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->deleteFile(Poco::Path(fname).toString());
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException(
                "Delete encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
    }

    void linkFile(const String & src_fname, const String & dst_fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->linkFile(Poco::Path(src_fname).toString(), Poco::Path(dst_fname).toString());
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException(
                "Link encryption info from file: " + src_fname + " to " + dst_fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
    }

    void renameFile(const String & src_fname, const String & dst_fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->renameFile(Poco::Path(src_fname).toString(), Poco::Path(dst_fname).toString());
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException(
                "Link encryption info from file: " + src_fname + " to " + dst_fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
    }

private:
    TiFlashServer * tiflash_instance_wrap;
};
} // namespace DB
