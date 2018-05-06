from __future__ import absolute_import, print_function

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import socket
import json

def main(consumer_key, consumer_secret, access_token, access_token_secret, IP, port, congress_data_path):
    # Go to http://apps.twitter.com and create an app.
    # The consumer key and secret will be generated for you after
    consumer_key=""
    consumer_secret=""

    # After the step above, you will be redirected to your app's page.
    # Create an access token under the the "Your access token" section
    access_token=""
    access_token_secret=""

    # load the congressional data from the given JSON file
    congress = loadCongressionalTwitters(congress_data_path)

    class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
    def on_data(self, data):
        try:
            msg = json.loads(data)
            tweet_favorites = msg['favorite_count']
            reply_user = "/RT/" + msg['in_reply_to_screen_name'].encode('utf-8')
            tweet_text_with_reply=msg['text'].encode('utf-8') + " " + reply_user
            print (msg['user']['screen_name'].encode('utf-8') + " --- "+ msg['text'].encode('utf-8') + str(tweet_favorites) +  "---" + reply_user)
            self.client_socket.send(tweet_text_with_reply)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    def on_error(self, status):
        print(status)
        return True

    def loadCongressionalTwitters(filepath):
        ''' 
        Loads information about members of congress from a JSON file, extract their twitter handles, adds an @ (to see targeted tweets) and returns it as a list
        The data file is from the ProPublica Congress API (https://projects.propublica.org/api-docs/congress-api/)
        '''
        with open(filepath) as congressData:
            congressData = (json.load(congressData))['results'][0]['members']
            output_dict = [x['twitter_account'] for x in congressData]
            handles = []
            for handle in output_dict:
                handle = "@" + str(handle)
                handles.append(handle)
            print(handles)
            return(handles)
    
    # adjusted this function to take a list as an argument so we can track large numbers of things at once -- in this case, tweets at the senate!
    def sendData(c_socket, trackList):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=trackList)

    # Additional fields, like only en tweets, in us 
    # twitter_stream.filter(track=['data'], languages=['en'], locations=[-130,-20,100,50])

    s = socket.socket()
    TCP_IP = IP
    TCP_PORT = port

    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    print("Wait here for TCP connection ...")

    conn, addr = s.accept()

    print("Connected, lets go get some tweets about congress!.")
    sendData(conn, congress)

if __name__ == "__main__":

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("consumer_secret")
     parser.add_argument("consumer_key")
    parser.add_argument("access_token")
    parser.add_argument("access_token_secret")
    parser.add_argument("--ip", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--dataPath", default ="./congress_data.json")
    args = parser.parse_args()

    # run the main function with the given command line arguments
    main(args.consumer_key, args.consumer_secret, args.access_token, args.access_token_secret, args.ip, args.port, args.dataPath)