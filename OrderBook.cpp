#include <string.h>
#include <sstream>

#include "OrderBook.h"

namespace OB {

	void init() 
	{ 
		OrderBook::get().initialize(); 
	}

	void destroy() 
	{ 
		OrderBook::get().shutdown(); 
	}

	/* Process an incoming limit order */
    QuoteProcessStatus processlimit(const std::string& quoteorder_) 
	{ 
		return OrderBook::get().limit(quoteorder_); 
	}

	void printOrderBook()
	{
		OrderBook::get().printOrderBook();
	}

	OrderBook& OrderBook::get() 
	{
		static OrderBook ob;
		return ob;
	}

	void OrderBook::initialize() 
	{
		_buyBook.clear();
		_askBook.clear();
		_lastbuyquote.clear();
		_lastsellquote.clear();
		 
		/* Initialize the price point array */
		_buyBook.resize(kMaxPrice);
		_askBook.resize(kMaxPrice);

		for (auto& pq : _buyBook) 
		{
			clearQueue(pq);
		}

		for (auto& pq : _askBook)
		{
			clearQueue(pq);
		}

		_askMin = _bidMin = kMaxPrice;
		_bidMax = _askMax  = kMinPrice;
	}

	void OrderBook::shutdown() {}

	QuoteProcessStatus OrderBook::limit(const std::string& quote_)
	{
		std::vector<std::string> tokens;

		bool ret = getQuoteAttributes(quote_, tokens);
		
		if (!ret)
		  return QuoteProcessStatus::MISSING_ATTRIBUTES;
		
		if (tokens[QUOTE].empty())
			return QuoteProcessStatus::INVALID_QUOTE;

		float price = static_cast<float>(atof(tokens[PRICE].c_str()));
		t_price priceslot = static_cast<t_price>(price * PRICEFACTOR);

		const char side = *(tokens[SIDE].c_str());

		if (side != 'A' && side != 'B')
			return QuoteProcessStatus::INVALID_SIDE;

		const char ordertype = *(tokens[ORDERTYPE].c_str());
		
		if (ordertype != 'N' && ordertype != 'U' && ordertype != 'D')
			return QuoteProcessStatus::INVALID_ORDERTYPE;

		const uint32_t volume = atol(tokens[VOLUME].c_str());

		if (ordertype != 'D')
		{
			if (volume == 0)
				return QuoteProcessStatus::INVALID_VOLUME;

			if (tokens[PRICE].empty())
				return QuoteProcessStatus::INVALID_PRICE;
		}
		
		if ( ordertype == 'N' )
		{
			return InsertQuoteIntoOrderBook(priceslot, tokens[QUOTE], side, ordertype, volume);
		}
		else if ( ordertype == 'U' )
		{
			return UpdateOrderBook(priceslot, tokens[QUOTE], side, ordertype, volume);
		}
		else if (ordertype == 'D' && priceslot == 0 && volume == 0)
		{
			if (!tokens[QUOTE].empty())
				return deleteQuotefromOrderBook(tokens[QUOTE], side);
			else
				return deleteOrderBook(side);
		}
		else
		{
			return QuoteProcessStatus::INVALID_ORDERTYPE;
		}
	}
}