#include "main.h"

// Convert DD/MM/YY to days from January 1, 1970
// First leap year adjustment is Mar 72 (26 months from Jan 70), then every 48 months
uint64_t USDate(uint64_t day, uint64_t month, uint64_t year) {
  uint64_t yearsFrom1970 = (year >= 70) ? year - 70 : year + 30;
  uint64_t leapAdjustment = ((12 * yearsFrom1970) + month + 21) / 48;
  uint64_t DaysFromNewYear [12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

  return (365 * yearsFrom1970) + leapAdjustment + DaysFromNewYear[month - 1] + (day - 1);
}


// Covert ip address string to uint
uint64_t IP_to_Uint(std::string & field) {
  std::string str;
  uint64_t val, value = 0;
  std::stringstream ss(field);

  for (uint64_t k = 0; k < 4; k ++) {     // ip4 addresses are 4 fields of 8 bits
      getline(ss, str, '.');
      try {val = stoull(str);}
      catch(...) {val = 0; break;}
      if (val < 256) value = (value << 8) + val;
      else {value = 0; break;}
  }

return value;
}


// Covert uint to ip address
std::string Uint_to_IP(uint64_t value) {
  std::string ipAddr = "";
  uint64_t octets[4];
  for (uint64_t k = 0; k < 4; k ++) {octets[k] = value & 255; value = value >> 8;}
  for (uint64_t k = 3; k >= 1; k --) ipAddr += std::to_string(octets[k]) + '.';
  return ipAddr + std::to_string(octets[0]);
}


uint64_t String_to_Uint(std::string & field, SchemaType type) {
  uint64_t value = ULONG_MAX;

  if (field == " ") {
     return value;

  } else if (type == UINT) {
     try {value = stoull(field);}
     catch(...) {value = 0;}

  } else if (type == CHARS) {
     memset(& value, '\0', sizeof(value));
     memcpy(& value, field.c_str(), 7);     // last char must be end-of-word

  } else if (type == STRING) {
     std::string * str = new std::string;
     * str = field;
     value = (uint64_t) str;

  } else if (type == INT) {
     int64_t this_value;
     try {this_value = stoll(field);}
     catch(...) {this_value = 0;}
     memcpy(& value, & this_value, sizeof(value));

  } else if (type == DOUBLE) {
     double this_value;
     try {this_value = stod(field);}
     catch(...) {this_value = 0.0;}
     memcpy(& value, & this_value, sizeof(value));

  } else if (type == COMPLEX) {
     double real, imag;
     std::vector <double> * listElems = new std::vector <double> (2);

     std::string plus_str ("+");
     std::string j_str    ("j");

     uint64_t plus = field.find(plus_str);
     uint64_t j = field.find(j_str);

     if ((plus == std::string::npos) && (j == std::string::npos)) {     // no plus and no j, number is real
        try {real = stod(field);}
        catch(...) {real = 0.0;}
        (* listElems)[0] = real;
        (* listElems)[1] = 0.0;
     } else if (j == std::string::npos) {                                // plus but no j, number is real
        try {real = stod(field.substr(0, plus));}
        catch(...) {real = 0.0;}
        (* listElems)[0] = real;
        (* listElems)[1] = 0.0;
     } else if (plus == std::string::npos) {                             // j but no plus, number is imaginary
        try {imag = stod(field.substr(0, j));}
        catch(...) {imag = 0.0;}
        (* listElems)[0] = 0.0;
        (* listElems)[1] = imag;
     } else {                                                           // plus and j, number is complex
        try {real = stod(field.substr(0, plus));}
        catch(...) {real = 0.0;}
        try {imag = stod(field.substr(plus + 1, std::string::npos));}
        catch(...) {imag = 0.0;}
        (* listElems)[0] = real;
        (* listElems)[1] = imag;
     }

     value = (uint64_t) listElems;

  } else if (type == BOOL) {
     if ((field == "T") || (field == "t") || (field == "TRUE") || (field == "true") || (field == "1")) value = 1;
     else value = 0;

  } else if (type == DATE) {
     struct tm date{};
     double this_value;
     date.tm_isdst = -1;
     strptime(field.c_str(), "%Y-%m-%d", & date);
     try {this_value = (double) mktime(& date);}
     catch(...) {this_value = 0.0;}
     memcpy(& value, & this_value, sizeof(value));

  } else if (type == USDATE) {
     struct tm date{};
     double this_value;
     date.tm_isdst = -1;
     strptime(field.c_str(), "%m/%d/%y", & date);
     try {this_value = (double) mktime(& date);}
     catch(...) {this_value = 0.0;}
     memcpy(& value, & this_value, sizeof(value));

  } else if (type == DATETIME) {
     struct tm date{};
     double this_value;
     date.tm_isdst = -1;
     strptime(field.c_str(), "%Y-%m-%dT%H:%M:%S", & date);
     try {this_value = (double) mktime(& date);}
     catch(...) {this_value = 0.0;}
     memcpy(& value, & this_value, sizeof(value));

  } else if (type == IPADDRESS) {
     value = IP_to_Uint(field);

  } else if (type == LIST_UINT) {
     std::string str;
     uint64_t this_value;
     std::vector <uint64_t> * listElems = new std::vector <uint64_t>;

     std::stringstream ss(field.substr(1, field.length() - 1));
     uint64_t nelems = count(field.begin(), field.end(), ',') + 1;

     for (uint64_t k = 0; k < nelems; k ++) {
         getline(ss, str, ',');
         try {this_value = stoull(str);}
         catch(...) {this_value = 0;}
         listElems->push_back(this_value);
     }
     value = (uint64_t) listElems;

  } else if (type == LIST_INT) {
     std::string str;
     int64_t this_value;
     std::vector <int64_t> * listElems = new std::vector <int64_t>;

     std::stringstream ss(field.substr(1, field.length() - 1));
     uint64_t nelems = count(field.begin(), field.end(), ',') + 1;

     for (uint64_t k = 0; k < nelems; k ++) {
         getline(ss, str, ',');
         try {this_value = stoll(str);}
         catch(...) {this_value = 0;}
         listElems->push_back(this_value);
     }
     value = (uint64_t) listElems;

  } else if (type == LIST_DOUBLE) {
      std::string str;
      double this_value;
      std::vector <double> * listElems = new std::vector <double>;

      std::stringstream ss(field.substr(1, field.length() - 1));
      uint64_t nelems = count(field.begin(), field.end(), ',') + 1;

      for (uint64_t k = 0; k < nelems; k ++) {
          getline(ss, str, ',');
          try {this_value = stod(str);}
          catch(...) {this_value = 0.0;}
          listElems->push_back(this_value);
      }
      value = (uint64_t) listElems;
  }

  return value;
}


std::string Uint_to_String(uint64_t value, SchemaType type) {
  std::ostringstream stream;
  if (value == ULLONG_MAX) return "";

  if (type == STRING) {
     stream << * ((std::string *) value);

  } else if (type == CHARS) {
     char * c = ((char *) & value);
     while (* c != '\0') {stream << * c; c ++;}

  } else if (type == UINT) {
     stream << value;

  } else if (type == INT) {
     stream << * ((int64_t *) (& value));

  } else if (type == DOUBLE) {
    double tmp;
    std::memcpy(&tmp, &value, sizeof(value));
    stream << std::fixed << std::setprecision(6) << tmp;

  } else if (type == COMPLEX) {
     std::vector <double> * listElems = (std::vector <double> *) value;
     stream << std::fixed << std::setprecision(6) << (* listElems)[0] << '+' << (* listElems)[1] << 'j';

  } else if (type == BOOL) {
      bool tmp = value != 0;
      stream << tmp;

  } else if (type == DATE) {
     struct tm * date{};
     char dateString[11];
     time_t time = static_cast<time_t>(*reinterpret_cast<double *>(&value));

     date = localtime(& time);
     strftime(dateString, 11, "%Y-%m-%d", date);
     stream << std::string((char *) dateString);

  } else if (type == USDATE) {
     struct tm * date{};
     char dateString[9];
     time_t time = static_cast<time_t>(*reinterpret_cast<double *>(&value));

     date = localtime(& time);
     strftime(dateString, 9, "%m/%d/%y", date);
     stream << std::string((char *) dateString);

  } else if (type == DATETIME) {
     struct tm * date{};
     char dateString[20];
     time_t time = static_cast<time_t>(*reinterpret_cast<double *>(&value));

     date = localtime(& time);
     strftime(dateString, 20, "%Y-%m-%dT%H:%M:%S", date);
     printf(dateString);
     stream << std::string((char *) dateString);

  } else if (type == IPADDRESS) {
     stream << Uint_to_IP(value);

  } else if (type == LIST_UINT) {
     std::vector <uint64_t> * listElems = (std::vector <uint64_t> *) value;

     stream << "{ ";
     for (uint64_t k = 0; k < listElems->size(); k ++) stream << (* listElems)[k] << ' ';
     stream << "}";

  } else if (type == LIST_INT) {
     std::vector <uint64_t> * listElems = (std::vector <uint64_t> *) value;

     stream << "{ ";
     for (uint64_t k = 0; k < listElems->size(); k ++) stream << (* listElems)[k] << ' ';
     stream << "}";

  } else if (type == LIST_DOUBLE) {
     std::vector <uint64_t> * listElems = (std::vector <uint64_t> *) value;

     stream << std::fixed << std::setprecision(6) << "{ ";
     for (uint64_t k = 0; k < listElems->size(); k ++) stream <<  (* listElems)[k] << ' ';
     stream << "}";

  }

  return stream.str();
}
