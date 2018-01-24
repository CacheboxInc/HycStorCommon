# Source
https://google.github.io/styleguide/cppguide.html

# Header Files

## Guard
use `#pragma once`

## Order of includes
1. C library
2. C++ library
3. other libraries' .h
4. your project's .h.

for example:
```
#include <sys/types.h>
#include <unistd.h>

#include <unordered_map>
#include <vector>

#include <folly/futures/Future.h>

#include "RequestHandler.h"
````

# Namespace
- never add `using namespace std` in header file
- all the code must be in `namespace pio`

# Local Variables
- Place a function's variables in the narrowest scope possible, and initialize variables in the declaration.
- C++ allows you to declare variables anywhere in a function. We encourage you to declare them in as local a scope as possible, and as close to the first use as possible

# Function Naming
- Functions starts with a capital letter
- Use capital letter for each new word.
- No underscores in function name.

```
eg: 
  const VmdkID& GetID() const noexcept;
  VmdkHandle GetHandle() const noexcept;
  int RequestComplete(std::unique_ptr<Request> reqp);
```

# Class, Struct, Type Names
- Type names starts with a capital letter
- Use capital letter for each new wor
- No underscores

```
For eg:
// classes and structs
class UrlTable { ...
class UrlTableTester { ...
struct UrlTableProperties { ...

// typedefs
typedef hash_map<UrlTableProperties *, string> PropertiesMap;

// using aliases
using PropertiesMap = hash_map<UrlTableProperties *, string>;

// enums
enum UrlTableErrors { ...
```
# Variable Names

## Common Variable Names
- The names of variables (including function parameters) and data members are all lowercase, with underscores between words
- Data members of classes (but not structs) additionally have trailing underscores.
For instance: `a_local_variable`, `a_struct_data_member`, `a_class_data_member_`.

## Class Data Members
- Data members of classes, both static and non-static, are named like ordinary nonmember variables, but with a trailing underscore.

```
eg
class TableInfo {
  ...
 private:
  string table_name_;  // OK - underscore at end.
  string tablename_;   // OK.
  static Pool<TableInfo>* pool_;  // OK.
};
```

## Struct data members
- Data members of struct are named like ordinary nonmember variables.
- Do not use trailing underscores that data members in classes have.

```
For eg.
struct UrlTableProperties {
  string name;
  int num_entries;
  static Pool<UrlTableProperties>* pool;
};
```

## Constant Names
Variables declared constexpr or const, and whose value is fixed for the duration of the program,
are named with a leading `k` followed by mixed case.

```
eg.
const int kDaysInAWeek = 7;
```

# Function Names
Regular functions have mixed case; accessors and mutators may be named like variables.

Ordinarily, functions should start with a capital letter and have a capital letter for each new word
(a.k.a. "Camel Case" or "Pascal case"). Such names should not have underscores. Prefer to capitalize
acronyms as single words (i.e. `StartRpc()`, not `StartRPC()`).

```
AddTableEntry()
DeleteUrl()
OpenFileOrDie()
```

# Enum Names
Enumerators (for both scoped and unscoped enums) should be named either like constants or like macros: either
`kEnumName` or `ENUM_NAME`.

Preferably, the individual enumerators should be named like constants. However, it is also acceptable to
name them like macros. The enumeration name, `UrlTableErrors` (and `AlternateUrlTableErrors`),
is a type, and therefore mixed case.

```
eg.
enum UrlTableErrors {
  kOK = 0,
  kErrorOutOfMemory,
  kErrorMalformedInput,
};

enum AlternateUrlTableErrors {
  OK = 0,
  OUT_OF_MEMORY = 1,
  MALFORMED_INPUT = 2,
};
```
# Line Length
Each line of text in your code should be at most 80 characters long.
