import tweepy
from textblob import TextBlob

consumer_key = 'Yf81pqPWPcjb9gZnpFv28cEkQ'
consumer_secret = 'VPjyrSuMMcA8ktskKec8h6X4TrkP7HQbwlpNMQJ7yjPiLFZtaq'

access_token = '3106982514-qvugiOJX2L0BYNDa7zcHEp0tLKmSwLdWG799Eec'
access_token_secret = 'SKlCX26KCeMjRebI6CNvAQn6aNYwdy1Vi1EK103C12Fnh'

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)

auth.set_access_token(access_token,access_token_secret)

api = tweepy.API(auth)

public_tweets = tweepy.Cursor(api.search, q='Trump', lang = "en").items(10)

for tweet in public_tweets:
	print(tweet.text)
	analysis = TextBlob(tweet.text)
	print(analysis.sentiment)

input("Press enter to exit")
