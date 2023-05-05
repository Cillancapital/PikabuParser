import asyncio
import time

import aiohttp
import random
import json

from bs4 import BeautifulSoup

PIKABU_DB_LIMIT = 300

TIME = 0


def tick(func):
    def wrapper(*args, **kwargs):
        go = time.time()
        result = func(*args, **kwargs)
        stop = time.time()
        it_took = stop - go
        print(f"{func.__name__} took", it_took)
        global TIME
        TIME += it_took
        return result
    return wrapper


class StoryCommentsParser:
    """
    It's a class that gets all the comments from a story from pikabu.ru by the story's id.
    """

    def __init__(self, story_id):
        self.story_id = story_id
        self.comments = self.proceed_get_story_comments_request()

        print(f'We got {len(self.comments)} comments in story No {self.story_id}')

    def proceed_get_story_comments_request(self):
        """
        The main method, that manages comments parsing
        :return: set of Comment objects

        """
        all_comments = []

        # Get all the root comments from the post
        root_comments = self.get_root_comments()
        root_cmnts_with_children, root_cmnts_without_children, comments_posts = self.divide_by_three_groups(
            root_comments)

        # Parse comments_posts
        if False:
            deep_comments = []
            for each_post in comments_posts:
                deep_comments.extend(StoryCommentsParser(each_post.id_post_comment).comments)
            all_comments.extend(deep_comments)

        # Get children
        children = asyncio.run(self.get_children(root_cmnts_with_children))

        all_comments.extend(root_comments)
        all_comments.extend(children)

        return all_comments

    def get_root_comments(self) -> list:
        """
        Gets all the root comments from the post. Completely ignores child comments.
        :return: a list of Comment objects
        """
        root_comments_list = []

        # First request goes without additional parameter "start_comment_id"
        comment_id_to_start_searching = ''
        number_of_comments_in_response = PIKABU_DB_LIMIT

        # Keep making requests while we get 300 comments by one request
        while number_of_comments_in_response == PIKABU_DB_LIMIT:
            # Make a request
            result = asyncio.run(self.make_request(action='get_story_comments',
                                                   start_comment_id=comment_id_to_start_searching))
            total_number_of_comments = result['total_number_of_comments']

            # Quit if post has no comments
            if total_number_of_comments == 0:
                return root_comments_list

            # Convert each root comment html to Comment class object
            parsed_comments = [Comment(each['html']) for each in result['list_of_comments']]
            root_comments_list.extend(parsed_comments)

            # Quit if post has few comments, and we got them all by one request
            if total_number_of_comments == len(root_comments_list):
                return root_comments_list

            # The id of the last root comment in the list
            comment_id_to_start_searching = parsed_comments[-1].id
            number_of_comments_in_response = len(root_comments_list)

        return root_comments_list

    @staticmethod
    def divide_by_three_groups(root_comments: list):
        """
        Divides all root comments by three groups:
            - comments with children
            - comments without children
            - comments - posts
        :param root_comments: List of Comment objects (root comments)
        :return: Three lists
        """
        comments_posts = [comment for comment in root_comments if comment.id_post_comment]
        root_cmnts_with_children = [comment for comment in root_comments if comment.has_children]
        root_cmnts_without_children = [comment for comment in root_comments
                                       if not comment.has_children and comment not in comments_posts]
        return root_cmnts_with_children, root_cmnts_without_children, comments_posts

    async def get_children(self, root_comments):
        # todo fix the docstring
        """
        Gets all the child comments from the root comments list (Comment object)
        Logic:
        The response is limited by 300 comments.
        It's ok if the 'get_comments_subtree' request returns less than 300 comments.
        If the request returns 300 comments, we make the other request 'get_story_comments' with
        start_comment_id = root comment id. It returns all his children with indent = 1 ????????????????????????
        :param root_comments:
        :return: list of child comments
        """
        children_to_return = []

        tasks = [self.make_request(action='get_comments_subtree', id=comment.id) for comment in root_comments]
        # child_comments is a list of huge htmls. Each html contains a comment tree for each root comment.
        child_comments = await asyncio.gather(*tasks)

        for each in child_comments:
            one_root_comment_children = BeautifulSoup(each, 'lxml').findAll(class_='comment')
            if len(one_root_comment_children) == 300:
                self.invent_a_function()
                print('Fuck! There were more than 300 children')
                # todo here i should create a function to get more than 300 children
            else:
                children_objects = [Comment(comment.prettify()) for comment in one_root_comment_children]
                children_to_return.extend(children_objects)

        return children_to_return

    def invent_a_function(self):
        pass

    @staticmethod
    def set_anti_ddos_headers():
        user_agent_list = ['Chrome/108.0.0.0',
                           'Mozilla/5.0',
                           'Dalvik/2.1.0',
                           ]
        return {'User-Agent': random.choice(user_agent_list)}

    async def make_request(self, action, **kwargs):
        php_request = 'https://pikabu.ru/ajax/comments_actions.php'
        req_params = {'action': action}

        # ?action=get_comments_subtree & id=102243651
        # id is needed to get the comment's subtree
        if action == 'get_comments_subtree':
            req_params['id'] = kwargs['id']

        # ?action=get_story_comments&story_id=5555555 & start_comment_id=102280013
        # story_id is required, start_comment_id or last_comment_id if needed only
        elif action == 'get_story_comments':
            req_params['story_id'] = self.story_id
            if kwargs.get('last_comment_id'):
                req_params['last_comment_id'] = kwargs['last_comment_id']
            if kwargs.get('start_comment_id'):
                req_params['start_comment_id'] = kwargs['start_comment_id']

        # request
        async with aiohttp.ClientSession() as session:
            async with session.post(php_request, data=req_params, params={'g': 'goog'},
                                    headers=self.set_anti_ddos_headers()) as response:
                result = await response.text()

        result_in_json = json.loads(result)

        data_to_return = {}
        if action == 'get_comments_subtree':
            data_to_return = result_in_json['data']['html']

        elif action == 'get_story_comments':
            data_to_return = {'total_number_of_comments': result_in_json['data']['total'],
                              'list_of_comments': result_in_json['data']['comments'],
                              }
        return data_to_return


class Comment:
    """
    Describes a comment. Contains all the useful info of the comment.
    To create takes html only, str format.
    Comment object has:
        - Author name and id
        - Comment content HTML
        - Comment metadata
        - Comment id, and it's parent ids
        - If has children
        - URL and id of a post if the comment is the post.

    """

    def __init__(self, parsed_comment: str):
        self.soup = BeautifulSoup(parsed_comment, 'lxml')
        self.content_tag_html = self.soup.find(class_='comment__content').prettify()
        self._clean_soup()

        self.metadata: dict = self._get_data_meta_from_soup()
        self.author: dict = self._get_author()
        self.id: int = int(self.soup.find('div', class_='comment').get('data-id'))
        self.parent_id: int = self.metadata['pid']
        self.has_children = self._has_children()

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

    def _has_children(self) -> bool:
        """
        Checks if comment has children by its html
        :return: True or False
        """
        children = self.soup.find('div', class_='comment-toggle-children')
        return True if children else False

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
    # a = StoryCommentsParser(story_id=10085566)  # https://pikabu.ru/story/biznes_ideya_10085566#comments 1900 comments
    a = StoryCommentsParser(story_id=10161553)  # 492 comments
    # a = StoryCommentsParser(story_id=5_555_555) #10 comments
    # a = StoryCommentsParser(story_id=6740346) #4000 comments badcomedian
    # a = StoryCommentsParser(story_id=10182975) #https://pikabu.ru/story/otzyiv_o_bmw_x6_10182975


