
#include <string>
#include "orderbook.h"

using namespace OB;

int main()
{
	std::string orderstr1 = "Q1/A/N/1.31/1000000";
	std::string orderstr2 = "Q2/B/N/1.21/1000000";
	std::string orderstr3 = "Q3/B/N/1.22/1000000";
	std::string orderstr4 = "Q4/B/N/1.20/1000000";
	std::string orderstr5 = "Q5/B/N/1.20/1000000";
	std::string orderstr6 = "Q6/A/N/1.32/1000000";

	std::string orderstr7 = "Q7/A/N/1.33/200000";
	std::string orderstr8 = "Q5/B/U/1.20/500000";
	std::string orderstr9 = "Q7/A/U/1.33/100000";
	std::string orderstr10 = "Q7/A/D/0/0";

	std::vector<std::string> orderstr = { orderstr1, orderstr2, orderstr3, orderstr4, orderstr5, orderstr6,
										  orderstr7, orderstr8, orderstr9, orderstr10 };

	init();

	QuoteProcessStatus status;
	CurrentTime localtime;

	for (auto ii = 0; ii < orderstr.size(); ii++)
	{
		std::cout << "\n\nSending " << orderstr[ii];
	    uint64_t starttime = localtime.microseconds();

		status = processlimit( orderstr[ii] );

	    uint64_t endtime = localtime.microseconds();

		std::cout << "\nTook " << endtime - starttime << " Microseconds to process this quote";

		if (status != QuoteProcessStatus::QUOTE_PROCESSED)
			std::cerr << "\nError: " << orderstr[ii];
		else
			std::cout << "\nLimit Order Successfully Processed\n";
	}

	printOrderBook();

	return 0;
}