

<h1> So you decided to pay taxes on your crypto :/ </h1>
<h2> Maybe you have a good reason for doing that...</h2>
<h3> Government is organized crime. Taxation is theft. </h3>

This program takes data from a variety of sources, both exchanges and wallets, aggregates it into a common format, then interprets crypto-crypto trades as 1041 like-kind exchanges and correctly keeps track of cost basis and origin dates for determination of long or short term capital gains on spending/sale of crypto. The results are stored in csvs. It also will fill out the 8824 forms for you and concatenate them into one giant pdf. Watch out tho, the IRS only lets you upload files up to something like 180MB to their system, so if it's bigger than that you may have to print it out and mail it in :D


<br><br>To see where to put your data, look at likekind.collect_transactions. If you need a different datasource supported, add it to parsers.py. If you have more coins that are not yet supported, you can add them to prices.get_name_symbol. If somehow the price data is not available on coinmarketcap or bitinfocharts for your coin, you can add another path to the switch in prices.get_price. Printing to screen slowing you down? See likekind.maybe_print. If you get errors saying directories don't exist, make them and put the right data in them. The 8824 folder holds all the stuff for filling out irs forms. You'll want to put your full name and social security number (Identifying number xxx-xx-xxxx) into 8824-blank.pdf before using it as a template.

<br><br> Once you get your csvs in the right place, install the requirements
<br>pip install -r requirements.txt
<br> and then just open an interpreter and
<br>>>import likekind
<br>>>likekind.start_to_finish()
