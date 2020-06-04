#pragma once

#include <Columns/Collator.h>
#include <common/StringRef.h>

#include <memory>

namespace TiDB
{

class ITiDBCollator : public ICollator
{
public:
    enum
    {
        UTF8_GENERAL_CI = 33,
        UTF8MB4_GENERAL_CI = 45,
        UTF8MB4_BIN = 46,
        LATIN1_BIN = 47,
        BINARY = 63,
        ASCII_BIN = 65,
        UTF8_BIN = 83,
    };

    /// Get the collator according to the internal collation ID, which directly comes from tipb and has been properly
    /// de-rewritten - the "New CI Collation" will flip the sign of the collation ID.
    static std::unique_ptr<ITiDBCollator> getCollator(int32_t id);

    class IPattern
    {
    public:
        virtual ~IPattern() = default;

        virtual void compile(const std::string & pattern, char escape) = 0;
        virtual bool match(const char * s, size_t length) const = 0;

    protected:
        IPattern() = default;
    };

    ~ITiDBCollator() override = default;

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override = 0;
    virtual StringRef sortKey(const char * s, size_t length, std::string & container) const = 0;
    virtual std::unique_ptr<IPattern> pattern() const = 0;
    int32_t getCollatorId() const { return collator_id; }

protected:
    explicit ITiDBCollator(int32_t collator_id_) : collator_id(collator_id_){};
    int32_t collator_id;
};

} // namespace TiDB