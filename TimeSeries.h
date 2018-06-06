/*
 * TimeSeries.h
 *
 *  Created on: Jun 20, 2017
 *      Author: mluk
 */

#ifndef CHALLENGE_TIMESERIES_H_
#define CHALLENGE_TIMESERIES_H_

#include <vector>
#include <stdexcept>
#include <iostream>
#include "VTime.h"
#include <limits>

template<typename>
struct default_value;

template<>
struct default_value<int> {
   static constexpr int value = 0;
};

template<>
struct default_value<double> {
   static constexpr double value = std::numeric_limits<double>::quiet_NaN();
};

template <typename T>
class TimeSeries
{
public:
    std::vector<uint64_t> timeUsSinceMid;
    std::vector<T> values;

    void addValue(uint64_t time, T value)
    {
        timeUsSinceMid.push_back(time);
        values.push_back(value);
    }
    size_t size() const
    {
        return values.size();
    }

    /*
     *
     *
     * @param sampleFreq
     * @param startTime
     * @return
     */
    TimeSeries<T>* sample(uint64_t sampleFreq, uint64_t startTime, uint64_t endTime)
	{
		TimeSeries<double> *timeseries = new TimeSeries<double>;

		auto cumtime = startTime;
		for (; cumtime < timeUsSinceMid[0]; cumtime += sampleFreq)
		{
			timeseries->addValue(cumtime, default_value<double>::value);
		}

		size_t next = 0;
		for (size_t ii = 0; ii < size() && cumtime < endTime; ii++)
		{
			if (timeUsSinceMid[ii] < cumtime)
				continue;

			timeseries->addValue(cumtime, values[ii]);
			cumtime += sampleFreq;
			next = ii + 1;

			if (ii != size() - 1)
			{
				while (cumtime > timeUsSinceMid[ii] && cumtime < timeUsSinceMid[next] && cumtime < endTime)
				{
					timeseries->addValue(cumtime, values[ii]);
					cumtime += sampleFreq;
				}
			}

			ii = next - 1;			
		}
			
		for (auto end = timeUsSinceMid[size() - 1]; end < endTime; end += sampleFreq)
		{
				timeseries->addValue(end, values[size() - 1]);
		}

		return timeseries;
    }

    TimeSeries<T>* sample(uint64_t sampleFreq)
    {
		return sample(sampleFreq, timeUsSinceMid[0], timeUsSinceMid[size() - 1]);
    }

};



template <typename T>
inline std::ostream& operator<<( std::ostream& out, const TimeSeries <T>& o  )
{
    for (int i = 0; i < o.size(); i++)
    {
        VTime v(o.timeUsSinceMid[i]);
        out << v.toHumanReadable()   << " " << o.values[i] << std::endl;
    }
    return out;
}

#endif 
