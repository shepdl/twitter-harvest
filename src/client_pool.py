import typing
import time
import datetime
import collections

from twitter import Twitter, OAuth2, oauth2_dance


class ClientPool:

    def __init__(self, config: typing.List):
        self.clients = collections.deque()
        for item in config:
            self.clients.append(
                ClientAvailableResult.available(Client(_TwitterCredentials(item), item['name']))
            )
        self.current_client = self.clients.popleft()
        print('Starting with {}'.format(self.current_client.client.name))

    def invalidate(self) -> None:
        self.current_client.available_requests = -1
        self._next_client()

    def _next_client(self):
        self.clients.append(self.current_client)
        self.current_client = self.clients.popleft()

    def available_client(self):
        self.current_client.mark_request()
        if self.current_client.has_available_requests():
        # if self.current_client.remaining_requests > 0:
            return self.current_client
        else:
            if self.current_client.in_new_window():
                self.current_client.reset()
            self._next_client()
            new_client = self.current_client
            print('Switching to {}'.format(new_client.client.name))
            if new_client.in_new_window(datetime.datetime.now()):
                new_client.reset()
            else:
                time_to_wait = datetime.datetime.now() - new_client.last_use_started
                if time_to_wait < ClientAvailableResult.WINDOW_SIZE:
                    print('Must rest client for {} seconds'.format(time_to_wait.total_seconds()))
                    time.sleep(time_to_wait.total_seconds())
                new_client.reset()
            return new_client

        # if a client is selected, check that
        # if it has remaining requests, then return it
        # otherwise, get next client in queue and put current client onto the end of the queue
        # if enough time has passed, return it
        # otherwise, return not_available


class ClientAvailableResult:

    def __init__(self, available: bool):
        self.available = available
        self.time_to_wait = 0
        self.client = None
        self.remaining_requests = 180
        self.remaining_requests = 450

    @staticmethod
    def not_available(time_to_wait):
        inst = ClientAvailableResult(False)
        inst.time_to_wait = time_to_wait
        inst.remaining_results = -1
        return inst

    @staticmethod
    def available(client):
        inst = ClientAvailableResult(True)
        inst.last_use_started = datetime.datetime.now()
        inst.client = client
        return inst

    WINDOW_SIZE = datetime.timedelta(minutes=15)

    def in_new_window(self, time: datetime.datetime = None) -> bool:
        time = time or datetime.datetime.now()
        return self.last_use_started is None or time - self.last_use_started > self.WINDOW_SIZE

    def reset(self):
        self.time_to_wait = 0
        self.remaining_requests = 450
        self.last_use_started = datetime.datetime.now()

    def has_available_requests(self):
        return self.remaining_requests > -1

    def mark_request(self):
        self.remaining_requests -= 1
        if self.last_use_started is None:
            self.last_use_started = datetime.datetime.now()


class _TwitterCredentials:

    def __init__(self, credentials):
        self.consumer_key = credentials['twitter_consumer_key']
        self.consumer_secret = credentials['twitter_consumer_secret']
        self.token = credentials['twitter_oauth_token']
        self.token_secret = credentials['twitter_oauth_token_secret']

    def to_dict(self):
        return {
            'consumer_key': self.consumer_key,
            'consumer_secret': self.consumer_secret,
            'token': self.token,
            'token_secret': self.token_secret,
        }


class Client:

    def __init__(self, credentials: _TwitterCredentials, name: str):
        self.credentials = credentials
        self.name = name
        self.last_made_active = None

    def to_twitter_client(self) -> Twitter:
        self.last_made_active = datetime.datetime.now()
        token = oauth2_dance(self.credentials.consumer_key, self.credentials.consumer_secret)

        # auth = OAuth(**self.credentials.to_dict())
        auth = OAuth2(bearer_token=token)
        twitter = Twitter(auth=auth)
        return twitter

