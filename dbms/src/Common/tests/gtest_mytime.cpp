#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace DB
{
namespace tests
{

class TestMyTime : public testing::Test
{
protected:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

public:
    static void checkParseMyDateTime(const std::string & str, const std::string & expected, const DataTypeMyDateTime & type)
    {
        try
        {
            UInt64 res = parseMyDateTime(str, type.getFraction()).template safeGet<UInt64>();
            MyDateTime datetime(res);
            std::string actual = datetime.toString(type.getFraction());
            EXPECT_EQ(actual, expected) << "Original datetime string: " << str;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << str << "\"" << std::endl;
            throw;
        }
    }

    static void checkParseMyDateTime(const std::string & str, MyDateTime & expected, const DataTypeMyDateTime & type)
    {
        try
        {
            UInt64 res = parseMyDateTime(str, type.getFraction()).template safeGet<UInt64>();
            MyDateTime source(res);
            EXPECT_EQ(source.year, expected.year) << "Original datetime string: " << str;
            EXPECT_EQ(source.month, expected.month) << "Original datetime string: " << str;
            EXPECT_EQ(source.day, expected.day) << "Original datetime string: " << str;
            EXPECT_EQ(source.hour, expected.hour) << "Original datetime string: " << str;
            EXPECT_EQ(source.minute, expected.minute) << "Original datetime string: " << str;
            EXPECT_EQ(source.second, expected.second) << "Original datetime string: " << str;
            EXPECT_EQ(source.micro_second, expected.micro_second) << "Original datetime string: " << str;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << str << "\"" << std::endl;
            throw;
        }
    }
};

TEST_F(TestMyTime, ParseMyDateTimeWithFraction)
try
{
    std::vector<std::tuple<std::string, std::string>> cases_with_fsp{
        {"2020-12-10 11:11:11.123456", "2020-12-10 11:11:11.123456"}, // YYYY-MM-DD HH:MM:SS.mmmmmm
        {"00-00-00 00:00:00.123", "2000-00-00 00:00:00.123000"},
        {"1701020304.1", "2017-01-02 03:04:01.000000"},
        {"1701020302.11", "2017-01-02 03:02:11.000000"},
        {"170102037.11", "2017-01-02 03:07:11.000000"},
        {"2018.01.01", "2018-01-01 00:00:00.000000"},
        {"2020.10.10 10.10.10", "2020-10-10 10:10:10.000000"},
        {"2020-10-10 10-10.10", "2020-10-10 10:10:10.000000"},
        {"2020-10-10 10.10", "2020-10-10 10:10:00.000000"},
        {"2018.01.01", "2018-01-01 00:00:00.000000"},
        {"2020--12-10 11:11:11..123456", "2020-12-10 11:11:11.123456"},
    };
    DataTypeMyDateTime type_with_fraction(6);
    for (auto & [str, expected] : cases_with_fsp)
    {
        checkParseMyDateTime(str, expected, type_with_fraction);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, ParseMyDateTimeWithoutFraction)
try
{
    std::vector<std::tuple<std::string, std::string>> cases_without_fsp{
        {"2012-12-31 11:30:45", "2012-12-31 11:30:45"},
        {"0000-00-00 00:00:00", "0000-00-00 00:00:00"},
        {"0001-01-01 00:00:00", "0001-01-01 00:00:00"},
        {"00-12-31 11:30:45", "2000-12-31 11:30:45"},
        {"12-12-31 11:30:45", "2012-12-31 11:30:45"},
        {"2012-12-31", "2012-12-31 00:00:00"},
        {"20121231", "2012-12-31 00:00:00"},
        {"121231", "2012-12-31 00:00:00"},
        {"2012^12^31 11+30+45", "2012-12-31 11:30:45"},
        {"2012^12^31T11+30+45", "2012-12-31 11:30:45"},
        {"2012-2-1 11:30:45", "2012-02-01 11:30:45"},
        {"12-2-1 11:30:45", "2012-02-01 11:30:45"},
        {"20121231113045", "2012-12-31 11:30:45"},
        {"121231113045", "2012-12-31 11:30:45"},
        {"2012-02-29", "2012-02-29 00:00:00"},
        {"00-00-00", "0000-00-00 00:00:00"},
        {"11111111111", "2011-11-11 11:11:01"},
        {"1701020301.", "2017-01-02 03:01:00"},
        {"170102036", "2017-01-02 03:06:00"},
        {"170102039.", "2017-01-02 03:09:00"},
        {"2018-01-01 18", "2018-01-01 18:00:00"},
        {"18-01-01 18", "2018-01-01 18:00:00"},
        {"2018.01.01 00:00:00", "2018-01-01 00:00:00"},
        {"2018/01/01-00:00:00", "2018-01-01 00:00:00"},
        {"4710072", "2047-10-07 02:00:00"},
    };
    DataTypeMyDateTime type_without_fraction(0);
    for (auto & [str, expected] : cases_without_fsp)
    {
        checkParseMyDateTime(str, expected, type_without_fraction);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, ParseMyDateTimeWithTimezone)
try
{
    std::vector<std::tuple<std::string, MyDateTime>> cases{
        {"2006-01-02T15:04:05Z", MyDateTime(2006, 1, 2, 15, 4, 5, 0)},
        {"2020-10-21T16:05:10Z", MyDateTime(2020, 10, 21, 16, 5, 10, 0)},
        {"2020-10-21T16:05:10.50+08", MyDateTime(2020, 10, 21, 8, 5, 10, 500 * 1000)},
        {"2020-10-21T16:05:10.50-0700", MyDateTime(2020, 10, 21, 23, 5, 10, 500 * 1000)},
        {"2020-10-21T16:05:10.50+09:00", MyDateTime(2020, 10, 21, 7, 5, 10, 500 * 1000)},
        {"2006-01-02T15:04:05+09:00", MyDateTime(2006, 1, 2, 6, 4, 5, 0)},
        {"2006-01-02T15:04:05-02:00", MyDateTime(2006, 1, 2, 17, 4, 5, 0)},
        {"2006-01-02T15:04:05-14:00", MyDateTime(2006, 1, 3, 5, 4, 5, 0)},
    };
    DataTypeMyDateTime type(6);
    for (auto & [str, expected] : cases)
    {
        checkParseMyDateTime(str, expected, type);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, Parser)
try
{
    std::vector<std::tuple<String, String, std::optional<MyDateTime>>> cases{
        {" 2/Jun", "%d/%b/%Y", MyDateTime{0, 6, 2, 0, 0, 0, 0}},  // More patterns than input string
        {" liter", "lit era l", MyDateTime{0, 0, 0, 0, 0, 0, 0}}, // More patterns than input string
        // Test case for empty input
        {"   ", " ", MyDateTime{0, 0, 0, 0, 0, 0, 0}},
        {"    ", "%d/%b/%Y", MyDateTime{0, 0, 0, 0, 0, 0, 0}},
        // Prefix white spaces should be ignored
        {"  2/Jun/2019 ", "%d/%b/%Y", MyDateTime{2019, 6, 2, 0, 0, 0, 0}},
        {"   2/Jun/2019 ", " %d/%b/%Y", MyDateTime{2019, 6, 2, 0, 0, 0, 0}},
        //
        {"31/May/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", MyDateTime{2016, 5, 31, 12, 34, 56, 123400}},
        {"31/may/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", MyDateTime{2016, 5, 31, 12, 34, 56, 123400}}, // case insensitive
        {"31/mayy/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", std::nullopt},                               // invalid %b
        {"31/mey/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", std::nullopt},                                // invalid %b
        {"30/April/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", MyDateTime{2016, 4, 30, 12, 34, 56, 0}},        // empty %f is valid
        {"30/april/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", MyDateTime{2016, 4, 30, 12, 34, 56, 0}},        // case insensitive
        {"30/Apri/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", std::nullopt},                                   // invalid %M
        {"30/Aprill/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", std::nullopt},                                 // invalid %M
        {"30/Feb/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f",
            MyDateTime{2016, 2, 30, 12, 34, 56, 123400}}, // Feb 30th (not exist in actual) is valid for parsing (in mariadb)
        {"31/April/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", MyDateTime{2016, 4, 31, 12, 34, 56, 0}}, // April 31th (not exist in actual)
        {"01,5,2013 9", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 900000}},
        {"01,52013", "%d,%c%Y", std::nullopt}, // %c will try to parse '52' as month and fail
        {"01,5,2013", "%d,%c,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},
        {"01,5,2013 ", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},

        /// Test cases for AM/PM set
        {"10:11:12 AM", "%H:%i:%S %p", std::nullopt}, // should not set %H %p at the same time
        {"10:11:12 Am", "%h:%i:%S %p", MyDateTime(0, 0, 0, 10, 11, 12, 0)},
        {"10:11:12 A", "%h:%i:%S %p", std::nullopt},                 // EOF while parsing "AM"/"PM"
        {"00:11:12 AM", "%h:%i:%S %p", std::nullopt},                // should not happen: %p set, %h not set
        {"11:12 AM", "%i:%S %p", std::nullopt},                      // should not happen: %p set, %h not set
        {"11:12 abcd", "%i:%S ", MyDateTime{0, 0, 0, 0, 11, 12, 0}}, // without %p, %h not set is ok
        {"00:11:12 ", "%h:%i:%S ", std::nullopt},                    // 0 is not a valid number of %h
        {"12:11:12 AP", "%h:%i:%S %p", std::nullopt},                // only AM/PM is valid
        {"12:11:12 AM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 0, 11, 12, 0)},
        {"12:11:12 PM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 12, 11, 12, 0)},
        {"11:11:12 pM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 23, 11, 12, 0)},
        /// Special case for %h with 12
        {"12:11:23 ", "%h:%i:%S ", MyDateTime(0, 0, 0, 0, 11, 23, 0)},
        // For %% -- FIXME: Ignored by now, both tidb and mariadb 10.3.14 can not handle it
        // {"01/Feb/2016 % 23:45:54", "%d/%b/%Y %% %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        // {"01/Feb/2016 %% 23:45:54", "%d/%b/%Y %%%% %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb/2016 % 23:45:54", "%d/%b/%Y %% %H:%i:%S", std::nullopt},
        {"01/Feb/2016 %% 23:45:54", "%d/%b/%Y %%%% %H:%i:%S", std::nullopt},

        /// Test cases for %r
        {" 04 :13:56 AM13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 4, 13, 56, 0}},
        {"13:13:56 AM13/5/2019", "%r", std::nullopt},  // hh = 13 with am is invalid
        {"00:13:56 AM13/05/2019", "%r", std::nullopt}, // hh = 0 with am is invalid
        {"00:13:56 pM13/05/2019", "%r", std::nullopt}, // hh = 0 with pm is invalid
        {"12: 13:56 AM 13/05/2019", "%r%d/%c/%Y", MyDateTime{2019, 5, 13, 0, 13, 56, 0}},
        {"12:13 :56 pm 13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 12, 13, 56, 0}},
        {"11:13: 56pm  13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 23, 13, 56, 0}},
        {"11:13:56a", "%r", std::nullopt}, // EOF while parsing "AM"/"PM"
        {"11:13", "%r", MyDateTime{0, 0, 0, 11, 13, 0, 0}},
        {"11:", "%r", MyDateTime{0, 0, 0, 11, 0, 0, 0}},
        {"12", "%r", MyDateTime{0, 0, 0, 0, 0, 0, 0}},

        /// Test cases for %T
        {" 4 :13:56 13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 4, 13, 56, 0}},
        {"23: 13:56  13/05/2019", "%T%d/%c/%Y", MyDateTime{2019, 5, 13, 23, 13, 56, 0}},
        {"12:13 :56 13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 12, 13, 56, 0}},
        {"19:13: 56  13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 19, 13, 56, 0}},
        {"21:13", "%T", MyDateTime{0, 0, 0, 21, 13, 0, 0}},
        {"21:", "%T", MyDateTime{0, 0, 0, 21, 0, 0, 0}},

        // mutiple chars between pattern
        {"01/Feb/2016 abcdefg 23:45:54", "%d/%b/%Y abcdefg %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        // the number of whitespace between pattern and input doesn't matter
        {"01/Feb/2016   abcdefg 23:45: 54", "%d/%b/%Y abcdefg %H  :%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb/  2016   abc  defg   23:45:54", "%d/  %b/%Y abcdefg %H:   %i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb  /2016   ab cdefg 23:  45:54", "%d  /%b/%Y abc  defg %H:%i  :%S", MyDateTime{2016, 2, 1, 23, 45, 54, 0}},

        /// Cases collect from MySQL 8.0 document
        {"01,5,2013", "%d,%m,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},
        {"May 1, 2013", "%M %d,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},
        {"a09:30:17", "a%h:%i:%s", MyDateTime{0, 0, 0, 9, 30, 17, 0}},
        {"a09:30:17", "%h:%i:%s", std::nullopt},
        {"09:30:17a", "%h:%i:%s", MyDateTime{0, 0, 0, 9, 30, 17, 0}},
        {"abc", "abc", MyDateTime{0, 0, 0, 0, 0, 0, 0}},
        {"9", "%m", MyDateTime{0, 9, 0, 0, 0, 0, 0}},
        {"9", "%s", MyDateTime{0, 0, 0, 0, 0, 9, 0}},
        // Range checking on the parts of date values is as described in Section 11.2.2, “The DATE, DATETIME, and TIMESTAMP Types”. This means, for example, that “zero” dates or dates with part values of 0 are permitted unless the SQL mode is set to disallow such values.
        {"00/00/0000", "%m/%d/%Y", MyDateTime{0, 0, 0, 0, 0, 0, 0}},
        {"04/31/2004", "%m/%d/%Y", MyDateTime{2004, 4, 31, 0, 0, 0, 0}},

        /// Below cases are ported from TiDB
        {"10/28/2011 9:46:29 pm", "%m/%d/%Y %l:%i:%s %p", MyDateTime(2011, 10, 28, 21, 46, 29, 0)},
        {"10/28/2011 9:46:29 Pm", "%m/%d/%Y %l:%i:%s %p", MyDateTime(2011, 10, 28, 21, 46, 29, 0)},
        {"2011/10/28 9:46:29 am", "%Y/%m/%d %l:%i:%s %p", MyDateTime(2011, 10, 28, 9, 46, 29, 0)},
        {"20161122165022", "%Y%m%d%H%i%s", MyDateTime(2016, 11, 22, 16, 50, 22, 0)},
        {"2016 11 22 16 50 22", "%Y%m%d%H%i%s", MyDateTime(2016, 11, 22, 16, 50, 22, 0)},   // fail, should ignore sep
        {"16-50-22 2016 11 22", "%H-%i-%s%Y%m%d", MyDateTime(2016, 11, 22, 16, 50, 22, 0)}, // fail, should ignore sep
        {"16-50 2016 11 22", "%H-%i-%s%Y%m%d", std::nullopt},
        {"15-01-2001 1:59:58.999", "%d-%m-%Y %I:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 999000)},
        {"15-01-2001 1:59:58.1", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 100000)},
        {"15-01-2001 1:59:58.", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 0)},
        {"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 9, 8, 999000)},
        {"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%S.%f", MyDateTime(2001, 1, 15, 1, 9, 8, 999000)},
        {"2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", std::nullopt}, // should not set %H %p at the same time
        {"10:20:10AM", "%H:%i:%S%p", std::nullopt},                       // should not set %H %p at the same time
        // test %@(skip alpha), %#(skip number), %.(skip punct)
        {"2020-10-10ABCD", "%Y-%m-%d%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-101234", "%Y-%m-%d%#", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10....", "%Y-%m-%d%.", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10.1", "%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"abcd2020-10-10.1", "%@%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"abcd-2020-10-10.1", "%@-%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10", "%Y-%m-%d%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10abcde123abcdef", "%Y-%m-%d%@%#", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
    };
    auto result_formatter = MyDateTimeFormatter("%Y/%m/%d %T.%f");
    size_t idx = 0;
    for (const auto & [input, fmt, expected] : cases)
    {
        MyDateTimeParser parser(fmt);
        auto packed = parser.parseAsPackedUInt(input);
        if (expected == std::nullopt)
        {
            MyTimeBase actual_time;
            String actual_str;
            if (packed)
            {
                actual_time = MyTimeBase(*packed);
                result_formatter.format(actual_time, actual_str);
            }
            EXPECT_FALSE((bool)packed) //
                << "[case=" << idx << "] "
                << "[fmt=" << fmt << "] [input=" << input << "] [actual=" << actual_str << "]";
        }
        else
        {
            MyTimeBase actual_time;
            String actual_str, expect_str;
            result_formatter.format(*expected, expect_str);
            if (packed)
            {
                actual_time = MyTimeBase(*packed);
                result_formatter.format(actual_time, actual_str);
                EXPECT_EQ(*packed, expected->toPackedUInt())
                    << "[case=" << idx << "] "
                    << "[fmt=" << fmt << "] [input=" << input << "] [expect=" << expect_str << "] [actual=" << actual_str << "]";
            }
            else
            {
                EXPECT_TRUE((bool)packed) //
                    << "[case=" << idx << "] "
                    << "[fmt=" << fmt << "] [input=" << input << "] [expect=" << expect_str << "] [actual=<parse fail>]";
            }
        }
        idx++;
    }
}
CATCH

} // namespace tests

} // namespace DB
