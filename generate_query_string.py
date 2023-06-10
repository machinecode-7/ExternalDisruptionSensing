def generate_QueryString(from_date_time, to_date_time, query_string, API_key):
        """
            - This method is responsible for generating the "GET" request parameter.
            - For every passed query string a get request url gets generated.
            - Below are input/output map of the function:
                Input : 
                    from_date_time : [STRING] -> A string representing the lower limit of the time window from which the news needs to be fetched.
                    to_date_time : [STRING] -> A string representing the upper limit of the time window uptill which the news needs to be fetched.
                    query_string : [STRING] -> The string consisting of appropriate filters that will be applied to return news articles. 
                Output :
                    queryString : [STRING] -> Fully qualified url along with HTTP GET request parameters.
        """
        queryString1 = ""
        queryString2 = ""
        queryString1 += "https://newsapi.org/v2/everything"
        queryString1 += "?"
        queryString2 += f"q={query_string}"
        queryString2 += f"&apiKey={API_key}"
        queryString2 += f"&searchIn=title,content,description"
        queryString2 += f"&from={from_date_time}"
        queryString2 += f"&to={to_date_time}"
        queryString2 += f"&language=en"
        queryString2 += f"&sortBy=relevancy"
        queryString = queryString1 + queryString2
        # print(f"Query String for News API between ({from_date_time} , {to_date_time}) : {queryString}")
        return queryString