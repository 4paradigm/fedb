/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_APISERVER_JSON_HELPER_H_
#define SRC_APISERVER_JSON_HELPER_H_

#include <cstddef>
#include <string>

namespace fedb {
namespace http {

/**
\class Archiver
\brief Archiver concept
Archiver can be a reader or writer for serialization or deserialization respectively.

Ref https://github.com/Tencent/rapidjson/blob/915218878afb3a60f343a80451723d023273081c/example/archiver/archiver.h
 **/

/// Represents a JSON reader which implements Archiver concept.
class JsonReader {
 public:
    /// Constructor.
    /**
        \param json A non-const source json string for in-situ parsing.
        \note in-situ means the source JSON string will be modified after parsing.
    */
    explicit JsonReader(const char* json);

    /// Destructor.
    ~JsonReader();

    // Archive concept

    operator bool() const { return !mError_; }

    JsonReader& StartObject();
    JsonReader& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonReader& EndObject();

    JsonReader& StartArray(size_t* size = nullptr);
    JsonReader& EndArray();

    JsonReader& operator&(bool& b);         // NOLINT
    JsonReader& operator&(unsigned& u);     // NOLINT
    JsonReader& operator&(int& i);          // NOLINT
    JsonReader& operator&(double& d);       // NOLINT
    JsonReader& operator&(std::string& s);  // NOLINT

    JsonReader& SetNull();

    static const bool IsReader = true;
    static const bool IsWriter = !IsReader;

 private:
    JsonReader(const JsonReader&);
    JsonReader& operator=(const JsonReader&);

    void Next();

    // PIMPL
    void* mDocument_;  ///< DOM result of parsing.
    void* mStack_;      ///< Stack for iterating the DOM
    bool mError_;       ///< Whether an error has occurred.
};

class JsonWriter {
 public:
    JsonWriter();
    ~JsonWriter();

    /// Obtains the serialized JSON string.
    const char* GetString() const;

    // Archive concept

    operator bool() const { return true; }

    JsonWriter& StartObject();
    JsonWriter& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonWriter& EndObject();

    JsonWriter& StartArray(size_t* size = nullptr);  // JsonReader needs size
    JsonWriter& EndArray();

    JsonWriter& operator&(const bool& b);
    JsonWriter& operator&(const unsigned& u);
    JsonWriter& operator&(const int& i);
    JsonWriter& operator&(const int64_t& i);
    JsonWriter& operator&(const double& d);
    JsonWriter& operator&(const std::string& s);
    JsonWriter& SetNull();

    static const bool IsReader = false;
    static const bool IsWriter = !IsReader;

    JsonWriter(const JsonWriter&) = delete;
    JsonWriter& operator=(const JsonWriter&) = delete;

 private:
    // PIMPL idiom
    void* mWriter_;  ///< JSON writer.
    void* mStream_;  ///< Stream buffer.
};

}  // namespace http
}  // namespace fedb

#endif  // SRC_APISERVER_JSON_HELPER_H_
