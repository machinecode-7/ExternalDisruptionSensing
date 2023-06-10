import json
import pandas as pd
from generate_query_list import generate_query_list
from generate_query_string import generate_QueryString
from Bard import Chatbot

token = "WwhAR33abISms9MLc0uWbYIVKuhsPs0Ze3v7mcKGwD7yJKL4CMNwwyRdvb9_Iiyq1njfEA."
bot = Chatbot(token)

with open("pipeline_configuration.json") as pipeline_config_fd:
        pipeline_config = json.load(pipeline_config_fd)
        API_KEY = pipeline_config["NewsAPI"]["api_key"]

client_data = pd.read_csv("client_master_data.csv")

query_list = generate_query_list(client_data)

responses = []
for query_string in query_list:
        qs = generate_QueryString("2023-06-09","2023-06-10", query_string, API_KEY)
        response = requests.request(url=qs, method="GET", verify=False)
        if response.status_code == 200:
                responses.append(response.json())

print(responses)

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


print(data)


