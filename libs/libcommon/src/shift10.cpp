// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <common/likely.h>
#include <common/shift10.h>

#include <limits>


template <typename T>
static T shift10Impl(T x, int exponent)
{
    static constexpr ssize_t MIN_EXPONENT = -323;
    static constexpr ssize_t MAX_EXPONENT = 308;

    static const long double powers10[]
        = {1e-323L, 1e-322L, 1e-321L, 1e-320L, 1e-319L, 1e-318L, 1e-317L, 1e-316L, 1e-315L, 1e-314L, 1e-313L, 1e-312L,
           1e-311L, 1e-310L, 1e-309L, 1e-308L, 1e-307L, 1e-306L, 1e-305L, 1e-304L, 1e-303L, 1e-302L, 1e-301L, 1e-300L,
           1e-299L, 1e-298L, 1e-297L, 1e-296L, 1e-295L, 1e-294L, 1e-293L, 1e-292L, 1e-291L, 1e-290L, 1e-289L, 1e-288L,
           1e-287L, 1e-286L, 1e-285L, 1e-284L, 1e-283L, 1e-282L, 1e-281L, 1e-280L, 1e-279L, 1e-278L, 1e-277L, 1e-276L,
           1e-275L, 1e-274L, 1e-273L, 1e-272L, 1e-271L, 1e-270L, 1e-269L, 1e-268L, 1e-267L, 1e-266L, 1e-265L, 1e-264L,
           1e-263L, 1e-262L, 1e-261L, 1e-260L, 1e-259L, 1e-258L, 1e-257L, 1e-256L, 1e-255L, 1e-254L, 1e-253L, 1e-252L,
           1e-251L, 1e-250L, 1e-249L, 1e-248L, 1e-247L, 1e-246L, 1e-245L, 1e-244L, 1e-243L, 1e-242L, 1e-241L, 1e-240L,
           1e-239L, 1e-238L, 1e-237L, 1e-236L, 1e-235L, 1e-234L, 1e-233L, 1e-232L, 1e-231L, 1e-230L, 1e-229L, 1e-228L,
           1e-227L, 1e-226L, 1e-225L, 1e-224L, 1e-223L, 1e-222L, 1e-221L, 1e-220L, 1e-219L, 1e-218L, 1e-217L, 1e-216L,
           1e-215L, 1e-214L, 1e-213L, 1e-212L, 1e-211L, 1e-210L, 1e-209L, 1e-208L, 1e-207L, 1e-206L, 1e-205L, 1e-204L,
           1e-203L, 1e-202L, 1e-201L, 1e-200L, 1e-199L, 1e-198L, 1e-197L, 1e-196L, 1e-195L, 1e-194L, 1e-193L, 1e-192L,
           1e-191L, 1e-190L, 1e-189L, 1e-188L, 1e-187L, 1e-186L, 1e-185L, 1e-184L, 1e-183L, 1e-182L, 1e-181L, 1e-180L,
           1e-179L, 1e-178L, 1e-177L, 1e-176L, 1e-175L, 1e-174L, 1e-173L, 1e-172L, 1e-171L, 1e-170L, 1e-169L, 1e-168L,
           1e-167L, 1e-166L, 1e-165L, 1e-164L, 1e-163L, 1e-162L, 1e-161L, 1e-160L, 1e-159L, 1e-158L, 1e-157L, 1e-156L,
           1e-155L, 1e-154L, 1e-153L, 1e-152L, 1e-151L, 1e-150L, 1e-149L, 1e-148L, 1e-147L, 1e-146L, 1e-145L, 1e-144L,
           1e-143L, 1e-142L, 1e-141L, 1e-140L, 1e-139L, 1e-138L, 1e-137L, 1e-136L, 1e-135L, 1e-134L, 1e-133L, 1e-132L,
           1e-131L, 1e-130L, 1e-129L, 1e-128L, 1e-127L, 1e-126L, 1e-125L, 1e-124L, 1e-123L, 1e-122L, 1e-121L, 1e-120L,
           1e-119L, 1e-118L, 1e-117L, 1e-116L, 1e-115L, 1e-114L, 1e-113L, 1e-112L, 1e-111L, 1e-110L, 1e-109L, 1e-108L,
           1e-107L, 1e-106L, 1e-105L, 1e-104L, 1e-103L, 1e-102L, 1e-101L, 1e-100L, 1e-99L,  1e-98L,  1e-97L,  1e-96L,
           1e-95L,  1e-94L,  1e-93L,  1e-92L,  1e-91L,  1e-90L,  1e-89L,  1e-88L,  1e-87L,  1e-86L,  1e-85L,  1e-84L,
           1e-83L,  1e-82L,  1e-81L,  1e-80L,  1e-79L,  1e-78L,  1e-77L,  1e-76L,  1e-75L,  1e-74L,  1e-73L,  1e-72L,
           1e-71L,  1e-70,   1e-69L,  1e-68L,  1e-67L,  1e-66L,  1e-65L,  1e-64L,  1e-63L,  1e-62L,  1e-61L,  1e-60L,
           1e-59L,  1e-58L,  1e-57L,  1e-56L,  1e-55L,  1e-54L,  1e-53L,  1e-52L,  1e-51L,  1e-50,   1e-49L,  1e-48L,
           1e-47L,  1e-46L,  1e-45L,  1e-44L,  1e-43L,  1e-42L,  1e-41L,  1e-40L,  1e-39L,  1e-38L,  1e-37L,  1e-36L,
           1e-35L,  1e-34L,  1e-33L,  1e-32L,  1e-31L,  1e-30,   1e-29L,  1e-28L,  1e-27L,  1e-26L,  1e-25L,  1e-24L,
           1e-23L,  1e-22L,  1e-21L,  1e-20L,  1e-19L,  1e-18L,  1e-17L,  1e-16L,  1e-15L,  1e-14L,  1e-13L,  1e-12L,
           1e-11L,  1e-10,   1e-9L,   1e-8L,   1e-7L,   1e-6L,   1e-5L,   1e-4L,   1e-3L,   1e-2L,   1e-1L,   1e0L,
           1e1L,    1e2L,    1e3L,    1e4L,    1e5L,    1e6L,    1e7L,    1e8L,    1e9L,    1e10,    1e11L,   1e12L,
           1e13L,   1e14L,   1e15L,   1e16L,   1e17L,   1e18L,   1e19L,   1e20L,   1e21L,   1e22L,   1e23L,   1e24L,
           1e25L,   1e26L,   1e27L,   1e28L,   1e29L,   1e30,    1e31L,   1e32L,   1e33L,   1e34L,   1e35L,   1e36L,
           1e37L,   1e38L,   1e39L,   1e40L,   1e41L,   1e42L,   1e43L,   1e44L,   1e45L,   1e46L,   1e47L,   1e48L,
           1e49L,   1e50,    1e51L,   1e52L,   1e53L,   1e54L,   1e55L,   1e56L,   1e57L,   1e58L,   1e59L,   1e60L,
           1e61L,   1e62L,   1e63L,   1e64L,   1e65L,   1e66L,   1e67L,   1e68L,   1e69L,   1e70,    1e71L,   1e72L,
           1e73L,   1e74L,   1e75L,   1e76L,   1e77L,   1e78L,   1e79L,   1e80L,   1e81L,   1e82L,   1e83L,   1e84L,
           1e85L,   1e86L,   1e87L,   1e88L,   1e89L,   1e90,    1e91L,   1e92L,   1e93L,   1e94L,   1e95L,   1e96L,
           1e97L,   1e98L,   1e99L,   1e100L,  1e101L,  1e102L,  1e103L,  1e104L,  1e105L,  1e106L,  1e107L,  1e108L,
           1e109L,  1e110,   1e111L,  1e112L,  1e113L,  1e114L,  1e115L,  1e116L,  1e117L,  1e118L,  1e119L,  1e120L,
           1e121L,  1e122L,  1e123L,  1e124L,  1e125L,  1e126L,  1e127L,  1e128L,  1e129L,  1e130,   1e131L,  1e132L,
           1e133L,  1e134L,  1e135L,  1e136L,  1e137L,  1e138L,  1e139L,  1e140L,  1e141L,  1e142L,  1e143L,  1e144L,
           1e145L,  1e146L,  1e147L,  1e148L,  1e149L,  1e150,   1e151L,  1e152L,  1e153L,  1e154L,  1e155L,  1e156L,
           1e157L,  1e158L,  1e159L,  1e160L,  1e161L,  1e162L,  1e163L,  1e164L,  1e165L,  1e166L,  1e167L,  1e168L,
           1e169L,  1e170,   1e171L,  1e172L,  1e173L,  1e174L,  1e175L,  1e176L,  1e177L,  1e178L,  1e179L,  1e180L,
           1e181L,  1e182L,  1e183L,  1e184L,  1e185L,  1e186L,  1e187L,  1e188L,  1e189L,  1e190,   1e191L,  1e192L,
           1e193L,  1e194L,  1e195L,  1e196L,  1e197L,  1e198L,  1e199L,  1e200L,  1e201L,  1e202L,  1e203L,  1e204L,
           1e205L,  1e206L,  1e207L,  1e208L,  1e209L,  1e210,   1e211L,  1e212L,  1e213L,  1e214L,  1e215L,  1e216L,
           1e217L,  1e218L,  1e219L,  1e220L,  1e221L,  1e222L,  1e223L,  1e224L,  1e225L,  1e226L,  1e227L,  1e228L,
           1e229L,  1e230,   1e231L,  1e232L,  1e233L,  1e234L,  1e235L,  1e236L,  1e237L,  1e238L,  1e239L,  1e240L,
           1e241L,  1e242L,  1e243L,  1e244L,  1e245L,  1e246L,  1e247L,  1e248L,  1e249L,  1e250,   1e251L,  1e252L,
           1e253L,  1e254L,  1e255L,  1e256L,  1e257L,  1e258L,  1e259L,  1e260L,  1e261L,  1e262L,  1e263L,  1e264L,
           1e265L,  1e266L,  1e267L,  1e268L,  1e269L,  1e270,   1e271L,  1e272L,  1e273L,  1e274L,  1e275L,  1e276L,
           1e277L,  1e278L,  1e279L,  1e280L,  1e281L,  1e282L,  1e283L,  1e284L,  1e285L,  1e286L,  1e287L,  1e288L,
           1e289L,  1e290,   1e291L,  1e292L,  1e293L,  1e294L,  1e295L,  1e296L,  1e297L,  1e298L,  1e299L,  1e300L,
           1e301L,  1e302L,  1e303L,  1e304L,  1e305L,  1e306L,  1e307L,  1e308L};

    if (unlikely(exponent < MIN_EXPONENT)) /// Note: there are some values below MIN_EXPONENT that is greater than zero.
        x *= 0; /// Multiplying to keep the sign of zero.
    else if (unlikely(exponent > MAX_EXPONENT))
        x *= std::numeric_limits<T>::infinity(); /// Multiplying to keep the sign of infinity.
    else
        x *= powers10[exponent - MIN_EXPONENT];

    return x;
}


double shift10(double x, int exponent)
{
    return shift10Impl(x, exponent);
}

float shift10(float x, int exponent)
{
    return shift10Impl(x, exponent);
}

double shift10(UInt64 x, int exponent)
{
    return shift10Impl(static_cast<long double>(x), exponent);
}

double shift10(Int64 x, int exponent)
{
    return shift10Impl(static_cast<long double>(x), exponent);
}
