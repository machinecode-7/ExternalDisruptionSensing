import requests
import json
import pandas as pd
from generate_query_list import generate_query_list
from generate_query_string import generate_QueryString
from Bard import Chatbot

def create_data(from_date, to_date, API_KEY):
        token = "WwhAR33abISms9MLc0uWbYIVKuhsPs0Ze3v7mcKGwD7yJKL4CMNwwyRdvb9_Iiyq1njfEA."
        bot = Chatbot(token)
        client_data = pd.read_csv("client_master_data.csv")

        query_list = generate_query_list(client_data)

        responses = []
        for query_string in query_list:
                qs = generate_QueryString(from_date, to_date, query_string, API_KEY)
                response = requests.request(url=qs, method="GET", verify=False)
                if response.status_code == 200:
                        responses.append(response.json())

        with open("test", "w") as fp:
                json.dump(responses, fp)


        with open("test", "r") as fp:
                responses = json.load(fp)

        data = {"url":[], "sentiment":[], "summary":[]}
        for response in responses:
                try:
                        url = response['articles'][0]['url']
                        # print(url)
                        # set your input text
                        input_text = f"is the sentiment of {url} positive, negative or neutral?"
                        output = bot.ask(input_text)['content']
                        sentiment = output.split("**")[1]
                        # print(url, " : ", sentiment)

                        summary_text = f"summarize {url} in less than 30 words."
                        summary = bot.ask(summary_text)['content']
                        summary = summary.split(":")[1].strip()

                        print(url, " : ", sentiment, " : ", summary)
                        data["url"].append(url)
                        data["sentiment"].append(sentiment)
                        data["summary"].append(summary)
                except Exception as e:
                        print(e)
                        
        pd.DataFrame(data).to_csv('report.csv')
        return data


