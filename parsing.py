import asyncio
import time

import aiohttp
import random
import json
import threading

from comment import Comment

TIME = 0
COUNT = 0


def tick(func):
    def wrapper(*args, **kwargs):
        go = time.time()
        result = func(*args, **kwargs)
        stop = time.time()
        it_took = stop - go
        print(f"{func.__name__} took", it_took)
        global TIME, COUNT
        TIME += it_took
        COUNT += 1
        return result

    return wrapper


class StoryCommentsParser:
    """
    A class that retrieves all comments from a story on pikabu.ru based on the story's ID.
    If `go_deep` is True, it also parses comment posts.

    Attributes:
        user_agent_list (list): A list of user agent strings.
        story_id (int): The ID of the story being parsed.
        total (int): The total number of comments in the post.
        min_id (int): The minimum comment ID.
        tree_structure (list): The tree structure of comments.
        comments (list): A list of Comment objects representing the comments in the story.

    Methods:
        _the_first_request(): Makes the initial request, retrieves necessary information to start the parsing process.
        _async_get_comments(): Retrieves a list of responses for the 'get_comments_by_ids' requests.
        _convert_raw_data_to_comment_objects(raw_data): Converts HTMLs to Comment objects.
        _group_comments_for_async_request(): Groups comment IDs for asynchronous requests.
        _get_all_ids_comments(): Retrieves all comment IDs from the tree structure of comments.
        _get_deep_comments(): Parses comment posts if `go_deep` is True.
        _make_request(**kwargs): Makes an asynchronous HTTP POST request.

    """
    user_agent_list = ['Chrome/108.0.0.0', 'Mozilla/5.0', 'Dalvik/2.1.0']

    def __init__(self, story_id, go_deep=False):
        """
        Initializes a StoryCommentsParser object.

        Args:
            story_id (int): The ID of the story to parse.
            go_deep (bool): Indicates whether to perform deep parsing of comment posts. Default is False.
        """
        # parse
        self.story_id = story_id
        self.total, self.min_id, self.tree_structure = self._the_first_request()
        raw_data = asyncio.run(self._async_get_comments())
        self.comments: list = self._convert_raw_data_to_comment_objects(raw_data)

        # Deep parse
        if go_deep:
            self._get_deep_comments()

        # Check
        if self.total == len(self.comments):
            print(f'All good {self.total=}')
        print(*[comment.id for comment in self.comments])

    def _the_first_request(self) -> (int, int, list):
        """
        Makes the very first request and retrieves the necessary information to start the parsing process.

        :return: A tuple containing the total number of comments in the post,
                 the minimum comment ID, and the comment tree structure.
        """
        req = asyncio.run(self._make_request(action='get_story_comments'))
        req = req['data']
        total = req['total']
        min_id = req['snapshot']['min']
        tree = req['snapshot']['tree']
        return total, min_id, tree

    async def _async_get_comments(self) -> tuple:
        """
        Retrieves a list of responses for the 'get_comments_by_ids' requests.

        :return: Tuple of dicts representing server responses. Each dict contains the following keys: 'result',
                 'message', 'message_code', and 'data'. The 'data' key provides access to a list of dicts,
                 where each dict contains the 'id' and 'html' keys.

        """
        # Get a list of dicts. Each dict contains keys: result, message, message code, data.
        # data contains a dict with keys: id and html
        tasks = [self._make_request(action='get_comments_by_ids',
                                    ids=','.join(group)) for group in self._group_comments_for_async_request()]
        responses = await asyncio.gather(*tasks)
        return responses

    @staticmethod
    @tick
    def _convert_raw_data_to_comment_objects(raw_data) -> list:
        """
        Converts HTMLs to Comment objects.

        :param raw_data: List of dicts representing server responses. Each dict contains the following keys: 'result',
                         'message', 'message_code', and 'data'. The 'data' key provides access to a list of dicts,
                         where each dict contains the 'id' and 'html' keys.
        :return: List of Comment objects.
        """
        responses: dict = raw_data

        list_of_comment_objects = []
        for response in responses:
            response = response['data']
            for comment in response:
                comment = comment['html']
                list_of_comment_objects.append(Comment(comment))
        return list_of_comment_objects

    @staticmethod
    @tick
    def _new_convert_logic(raw_data):
        """
        9 seconds for 4000 comments
        """
        def create_object(com):
            return Comment(com)

        results = []

        def worker(com):
            result = create_object(com)
            results.append(result)

        for each in raw_data:
            each = each['data']

            threads = []
            for comment in each:
                comment = comment['html']
                thread = threading.Thread(target=worker, args=(comment,))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

        return results

    @staticmethod
    async def _new_convert_logic_async(raw_data):
        """
        33 second for 4000 comments
        """
        start = time.time()
        responses: dict = raw_data
        comments_to_return = []
        htmls = []
        for response in responses:
            response = response['data'] # response - list of comments dicts
            htmls.extend([comment['html'] for comment in response])

        loop = asyncio.get_event_loop()
        comments = await asyncio.gather(
            *[loop.run_in_executor(None, Comment, parsed_comment) for parsed_comment in htmls])
        comments_to_return.extend(comments)
        print(f'{time.time() - start} logic2 async')
        return comments_to_return

    def _group_comments_for_async_request(self) -> list:
        """
        Groups comment IDs for asynchronous requests.

        :return: A list of lists, where each inner list represents a group of comment IDs
                 to be included in an asynchronous request.
        """
        i = 0
        comments_ids = self._get_all_ids_comments()
        groups = []
        while i < len(comments_ids):
            group = comments_ids[i:i + 300]
            group = [str(com_id) for com_id in group]
            groups.append(group)
            i += 300
        return groups

    def _get_all_ids_comments(self) -> list:
        """
        Retrieves all comment IDs from the tree structure of comments.

        :return: A sorted list of all comment IDs in the post.
        """

        def brake_down_the_structure(lst):
            """
            Recursively extracts all numbers from the tree structure.
            Skips the second element in each list because it's purpose is unknown.

            Structure example:
            [2564, 0, [[16208, 0, [[52051, 0,
            [[71927, 0, [[75123, 0]]]]]]], [34431, 0], [73023, 0], [14047, 0, [[177804, 0]]]]]

            :param lst: The list representing the tree structure.
            """

            for position, elem in enumerate(lst):
                if position == 1 and isinstance(elem, int):
                    continue
                elif isinstance(elem, list):
                    yield from brake_down_the_structure(elem)
                else:
                    yield elem

        snap = self.tree_structure
        broken = [self.min_id]
        for each in snap:
            break_one = [i + self.min_id for i in brake_down_the_structure(each)]
            broken.extend(break_one)
        list_of_all_comment_ids_in_the_post = list(broken)
        return sorted(list_of_all_comment_ids_in_the_post)

    def _get_deep_comments(self) -> None:
        """
        Parses comment posts if `go_deep` is set to `True`.
        Extends the list of comment objects in `self`.

        Note: Only comments with `id_post_comment` are considered for parsing.
        """
        comments_posts = [comment for comment in self.comments if comment.id_post_comment]
        for each_post in comments_posts:
            self.comments.extend(StoryCommentsParser(each_post.id_post_comment).comments)

    async def _make_request(self, **kwargs) -> dict:
        """
        Makes an asynchronous HTTP POST request.

        :param kwargs: Additional parameters for the request.
                       For 'get_story_comments' action:
                           - story_id: ID of the story.
                           - last_comment_id (optional): ID of the last comment.
                           - start_comment_id (optional): ID of the start comment.
                       For 'get_comments_by_ids' action:
                           - ids: Comma-separated IDs of the comments.
                       For 'get_comments_subtree' action:
                           - id: ID of the comment.

        :return: The JSON response from the server.
        """
        url = 'https://pikabu.ru/ajax/comments_actions.php'
        req_params = {'action': kwargs['action']}

        if kwargs['action'] == 'get_story_comments':
            req_params.update({'story_id': self.story_id,
                               'last_comment_id': kwargs['last_comment_id'] if kwargs.get('last_comment_id') else '',
                               'start_comment_id': kwargs['start_comment_id'] if kwargs.get('start_comment_id') else '',
                               })
        elif kwargs['action'] == 'get_comments_by_ids':
            req_params.update({'ids': kwargs['ids']})
        elif kwargs['action'] == 'get_comments_subtree':
            req_params.update({'id': kwargs['id'],
                               })

        async with aiohttp.ClientSession() as session:
            async with session.post(url,
                                    data=req_params,
                                    params={'g': 'goog'},
                                    headers={'User-Agent': random.choice(self.user_agent_list)},
                                    ) as response:
                result = await response.text()
        result_in_json = json.loads(result)
        return result_in_json




if __name__ == '__main__':
    # a = StoryCommentsParser(story_id=10085566)  # https://pikabu.ru/story/biznes_ideya_10085566#comments 1900 comments
    #a = StoryCommentsParser(story_id=10161553)  # 492 comments
    # a = StoryCommentsParser(story_id=5_555_555) #10 comments
    # a = StoryCommentsParser(story_id=10182975) #https://pikabu.ru/story/otzyiv_o_bmw_x6_10182975
    a = StoryCommentsParser(story_id=6740346) #4000 comments badcomedian
    # a = StoryCommentsParser(story_id=10208614, go_deep=True) # a lot of posts
    # a = StoryCommentsParser(story_id=10216528)
