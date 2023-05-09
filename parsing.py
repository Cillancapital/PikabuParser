import asyncio
import time

import aiohttp
import random
import json

from bs4 import BeautifulSoup

PIKABU_DB_LIMIT = 300

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

@tick
class StoryCommentsParser:
    """
    It's a class that gets all the comments from a story from pikabu.ru by the story's id.
    If go_deep starts comments-post parsing
    """
    user_agent_list = ['Chrome/108.0.0.0', 'Mozilla/5.0', 'Dalvik/2.1.0',]

    def __init__(self, story_id, go_deep=False):
        self.story_id = story_id
        self.total, self.min_id, self.tree_structure = self.the_first_request()
        self.comments = asyncio.run(self.async_get_comments())

        print(f'{self.total=}, We got {len(self.comments)}')

    def the_first_request(self) -> (int, int, list):
        """
        Makes the very first request to start the parsing.
        :return: A tuple of total amount of comments in the post, min comment id and the three structure.
        """
        req = asyncio.run(self.make_request(action='get_story_comments'))
        req = req['data']
        total = req['total']
        min_id = req['snapshot']['min']
        tree = req['snapshot']['tree']
        return total, min_id, tree

    async def async_get_comments(self):
        # Get a list of dicts. Each dict contains keys: result, message, message code, data.
        # data contains a dict with keys id and html
        tasks = [self.make_request(action='get_comments_by_ids',
                                   ids=','.join(group)) for group in self.group_comments_for_async_request()]
        responses = await asyncio.gather(*tasks)


        start = time.time()
        # Convert all htmls to Comment objects from responses
        list_of_comment_objects = []
        for response in responses:

            response = response['data']

            for comment in response:
                comment = comment['html']
                list_of_comment_objects.append(Comment(comment))

        print(f'It took {time.time() - start} sec to convert')
        return list_of_comment_objects

    def group_comments_for_async_request(self):
        i = 0
        comments_ids = self.get_all_ids_comments()
        groups = []
        while i < len(comments_ids):
            group = comments_ids[i:i+300]
            group = [str(com_id) for com_id in group]
            groups.append(group)
            i += 300
        return groups

    def get_all_ids_comments(self) -> list:
        """
        Gets all the ids from the tree of comments.
        :return: a list of comment ids
        """
        def brake_down_the_structure(lst):
            for elem in lst:
                if isinstance(elem, list):
                    yield from brake_down_the_structure(elem)
                else:
                    yield elem

        snap = self.tree_structure
        broken = []
        for each in snap:
            break_one = [i + self.min_id for i in brake_down_the_structure(each) if i != 0]
            broken.extend(break_one)
        list_of_all_comment_ids_in_the_post = list(set(broken))

        return sorted(list_of_all_comment_ids_in_the_post)

    def get_deep_comments(self):
        # todo docstring
        deep_comments = []
        for each_post in self.posts:
            deep_comments.extend(StoryCommentsParser(each_post.id_post_comment).comments)
        return deep_comments

    @staticmethod
    def set_anti_ddos_headers():
        user_agent_list = ['Chrome/108.0.0.0',
                           'Mozilla/5.0',
                           'Dalvik/2.1.0',
                           ]
        return {'User-Agent': random.choice(user_agent_list)}

    async def make_request(self, **kwargs):
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


class Comment:
    """
    Describes a comment. Contains all the useful info of the comment.
    To create takes html only, str format.
    Comment object has:
        - Author name and id
        - Comment content HTML
        - Comment metadata
        - Comment id, and it's parent ids
        - URL and id of a post if the comment is the post.

    """

    def __init__(self, parsed_comment: str):
        self.soup = BeautifulSoup(parsed_comment, 'lxml')

        # todo delete raw_html. Debug use only, critically slows down tho program
        # self.raw_html = self.soup.prettify()

        self.content_tag_html = self.soup.find(class_='comment__content').prettify()
        self._clean_soup()

        self.metadata: dict = self._get_data_meta_from_soup()
        self.author: dict = self._get_author()
        self.id: int = int(self.soup.find('div', class_='comment').get('data-id'))
        self.parent_id: int = self.metadata['pid']

        # Check if the comment is a post. In such case comment has the post url and id.
        # Otherwise, url = '', id = 0
        self.url_post_comment: str = self._is_post()
        self.id_post_comment: int = self._get_id_post_comment()

        self._delete_useless()

    def _clean_soup(self):
        """
        Cleans all unnecessary text from html.
        Deletes tags:
            comment__children
            comment__tools
            comment__controls
            comment__content
        """
        classes_to_delete = ('comment__children', 'comment__tools',
                             'comment__controls', 'comment__content')
        for class_ in classes_to_delete:
            tag_to_delete = self.soup.find(class_=class_)
            if tag_to_delete:
                tag_to_delete.extract()

    def _get_data_meta_from_soup(self) -> dict:
        """
        Gets all data_meta from the html.
        Html Example:
        <div class="comment"  id="comment_271331177" data-id="271331177" data-author-id="2927026"
        data-author-avatar="https://cs.pikabu.ru/images/def_avatar/def_avatar_80.png"
        data-meta="pid=0;aid=2927026;sid=10167269;said=6487807;d=2023-04-22T08:11:16+03:00;de=0;ic=0;r=6;av=6,0"
        data-story-subs-code="0" data-indent="0">

        data-meta explication:
        pid=0;                             - 0 if root comment, parent_id if has parent
        aid=1381602;                       - id of the author of the comment
        sid=10085566;                      - id of the story where the comment was publishes
        said=4874925;                      - id of the author of the story where the comment was publishes
        d=2023-03-28T17:41:30+03:00;       - date
        de=0;                              - 0 if not deleted, 1 if deleted
        ic=0;                              - No idea :(
        r=1294;                            - total rairing of the comment
        av=1367,73;                        - votes for + / votes for -
        hc                                 - head comment may be? no idea :(
        avh=-20282962854:-20282963014      - no idea :(

        av in data_meta dict is divided to av+ and av- (votes in favor, votes against)

        :return: data_meta dict
        """
        # Raw data-meta is a string 'pid=0;aid=3296271;sid=10085566;said=4874925;...'
        data_meta: str = self.soup.find('div', class_='comment').get('data-meta')
        data_meta: list = data_meta.split(';')

        data_meta: dict = {data.split('=')[0]: data.split('=')[1] for data in data_meta if '=' in data}

        # Make rating data look good
        try:
            vote_up, vote_down = data_meta['av'].split(',')
            data_meta.pop('av')
            data_meta['av+'] = vote_up
            data_meta['av-'] = vote_down
        except KeyError:
            data_meta['r'] = None
            data_meta['av+'] = None
            data_meta['av-'] = None

        # Convert all values to int format if possible
        for key, value in data_meta.items():
            try:
                data_meta[key] = int(value)
            except (ValueError, TypeError):
                pass

        return data_meta

    def _get_author(self) -> dict:
        """
        Gets the info about author
        Returns a dict with name and id
        """
        user_tag = self.soup.find(class_='comment__user')
        return {'name': user_tag.get('data-name'),
                'id': int(user_tag.get('data-id'))}

    def _is_post(self) -> str:
        """
        Checks if comments is a post by its html.
        :return: post-url if a post or '' if not a post
        """
        post = self.soup.find('div', class_='comment_comstory')
        return post.get('data-url') if post else ''

    def _get_id_post_comment(self) -> int:
        """
        Gets post_id of the post if the comment is a post
        """
        if self.url_post_comment:
            id_post = int(self.url_post_comment[self.url_post_comment.rfind('_') + 1:])
        else:
            id_post = 0
        return id_post

    def _delete_useless(self):
        """
        Deletes useless information to save memory
            - self soup
        """
        del self.soup


if __name__ == '__main__':
    #a = StoryCommentsParser(story_id=10085566)  # https://pikabu.ru/story/biznes_ideya_10085566#comments 1900 comments
    #a = StoryCommentsParser(story_id=10161553)  # 492 comments
    #a = StoryCommentsParser(story_id=5_555_555) #10 comments
    a = StoryCommentsParser(story_id=6740346) #4000 comments badcomedian
    # a = StoryCommentsParser(story_id=10182975) #https://pikabu.ru/story/otzyiv_o_bmw_x6_10182975

    #a = StoryCommentsParser(story_id=10219957) # a lot of posts
