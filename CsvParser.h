#pragma once
// C++ routine to read a CSV file , perform arthimetic operations on first two columns, output on fourth column, with third column reserved for operation
// this will be support floating point numbers and at the end median, average, min, max for a column will be outputted
// output will be written to the same file

/*****The general algorithm is that it reads the whole file into a buffer. Then it goes through the buffer setting \0 where the delimiter is ",". The delimeters
***********************can be changed on the demands of the format******************************************************************
***********************It can be easily extended to include multiple columns for input against which the operation will be performed***************************************
****/

/**The routine should handle files with lines like(input.csv)

13.2, 12.1, +
3, 2, *
8, 2, -
**/

#pragma once

#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <numeric>
#include <string>

/*** location provides a simple means to keep track of the tokens
the location class is used internally to keep track of the position in the CSV-file which is later used to retrieve the tokens. ***/
namespace csv {

	class location
	{
	public:
		location() : _row(0), _col(0) { ; }

		void newRow() { ++_row; _col = 0; }
		void newCol() { ++_col; }

		void resetCol() { _col = 0; }

		size_t col() const { return _col; }
		size_t row() const { return _row; }

	protected:
		size_t _row;
		size_t _col;
	};

	using fileMap_t = std::vector<char *>;
	constexpr uint8_t OPCOL = 2;

	class CsvParser
	{
	public:
		CsvParser(const std::string& fullPath);
		~CsvParser();
		CsvParser() = default;

		bool parseCSV(); /// main function, use index r*_cols+c to retrieve m[r,c]
		void writetofile();
		bool readFile();

		void innerjoin(size_t col1_, size_t col2_);
		void outerjoin(size_t col1_, size_t col2_, bool leftjoin);

		size_t rows() const;
		size_t cols() const;
		size_t emptyRows() const;

		inline const std::vector<char>& getfilebuffer() const 
		{ 
			return _fileBuffer; 
		}

		char* token(size_t row, size_t col); ///! can be used to access tokens once parseCSV has been called.

		void dumpfile();

		const double epsilon = 1.0e-10;

		inline bool IsEqual(double a, double b)
		{
			return (a > b) ? (a - b < epsilon) : (b - a < epsilon);
		}
	protected:
		void determineRowsAndCols(const char* fileBuffer, const std::streamoff length);
		void performOperation(const location& loc);

		void quicksort(std::vector<double>& vec, size_t L, size_t R);

		void writeRowtoOutputBuffer(const size_t row);
		void writeLastColtoOutputBuffer(const size_t row_);
		void writetoOutputBuffer(const size_t row_, const size_t col_);
		void writetovalueArray(const size_t row_, const size_t col_);
		void writeOutputBufferToFile();
		void writeStatString(const std::string&);

		double calcMedian(std::vector<double>& columns);

		size_t _rows;
		size_t _cols;
		size_t _emptyRows;
		std::streamoff _fileLength;
		std::vector<char> _fileBuffer;
		std::vector<char> _outputBuffer;
		std::vector<char> _innerJoinBuffer;
		std::vector<char> _outerJoinBuffer;
		fileMap_t _csvFile;

		using stats_t =  std::vector<std::string >;
		using matrix_t = std::vector<std::vector<double>>;

		enum Stats { Min, Max, Median, Avg, COUNTSTATS};

		std::vector<double> results; // holds the results of columns operation for each row ( row 0..m )

		std::string	  _fullPath;
		matrix_t      _valarrayrow;
		matrix_t      _valarraycol;
		stats_t       _stats{ "Min: ", "Max: " , "Median: ","Average: " };
	};
}
/*** Usage:*** CsvParser P("/Users/ak/SampleData.csv");
* fileMap_t fileContent = P.parseCSV();
* for (size_t row = 0; row < P.rows(); ++rows) *
* for (size_t col = 0; col < P.cols(); ++cols) *
* char* tok = fileContent[col + row*P.cols()]; ** or
* CsvParser P("/Users/ak/SampleData.csv"); *
* for (size_t row = 0; row < P.rows(); ++rows) *
* for (size_t col = 0; col < P.cols(); ++cols) *
* char* tok = P.token(row,col);
*********/