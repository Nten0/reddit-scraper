# reddit-scraper
In the first part of the project a search engine was developed. Using Scrapy, the news page of reddit was crawled and several articles were obtained and transformed in JSON files. Goose Extractor was used to isolate the text information of each article and the results were represented in the vector space model. A reverse file was also created and a copy of it was saved in a MySql database. A simple PHP script communicates with the database and returns to the end user a number of relevant articles with their query.

In the second part, a number of emails are loaded as training data and after representing them using the vector space model, we provide test data and calculate the efficiency of different metrics of similarity (cosine, Tanimoto and Jaccard).
