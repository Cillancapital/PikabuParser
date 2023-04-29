import asyncio
import time

import aiohttp
import random
import json

from bs4 import BeautifulSoup

PIKABU_DB_LIMIT = 300


class Comment:
    def __init__(self, parsed_comment: dict):
        # Parsed info
        self.id: int = parsed_comment['id']
        self.parent_id: int = parsed_comment['parent_id']
        self.raw_html: str = parsed_comment['html']

        # no idea what is_hidden and is_hidden_children serve for
        self.is_hidden: bool = parsed_comment['is_hidden']
        self.is_hidden_children: bool = parsed_comment['is_hidden_children']

        # Info from raw_html
        # Some comments could be written as a new story (a post)
        # Check if the comment is a post. In such case comment has the post's url and id.
        # Otherwise, url = '', id = '0'
        self.post_url_if_comment_is_post: str = self.is_post()
        if self.post_url_if_comment_is_post:
            self.post_id_if_comment_is_post: str = self.post_url_if_comment_is_post[
                                                   self.post_url_if_comment_is_post.rfind("_") + 1:]
        else:
            self.post_id_if_comment_is_post = 0

        self.metadata = self.get_data_meta_from_raw_html()

    def is_post(self) -> str:
        """
        Checks if comments is a post by its html.
        :return: False if it's not a post, story id if a post.
        """
        if '<div class="comment comment_comstory"' in self.raw_html:
            post_url_start_pos = self.raw_html.find('data-url=') + 10
            post_url_end_pos = self.raw_html.find('data-story-subs-code') - 2
            post_url = self.raw_html[post_url_start_pos:post_url_end_pos]
            return post_url
        return ''

    def has_children(self) -> bool:
        """
        Checks if comment has children by its html
        :return: True or False
        """
        return True if 'comment-toggle-children' in self.raw_html else False

    def get_data_meta_from_raw_html(self):
        """
        Gets all data_meta from the html.
        Html Example:
        <div class="comment"  id="comment_271331177" data-id="271331177" data-author-id="2927026"
        data-author-avatar="https://cs.pikabu.ru/images/def_avatar/def_avatar_80.png"
        data-meta="pid=0;aid=2927026;sid=10167269;said=6487807;d=2023-04-22T08:11:16+03:00;de=0;ic=0;r=6;av=6,0"
        data-story-subs-code="0" data-indent="0">

        data-meta explication:
        pid=0;                             - 0 - 0 if root comment, 1 if has parent
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

        :return: Data-meta dict
        """
        start_pos = self.raw_html.find('pid=')
        data_meta = self.raw_html[start_pos:].split(' ')[0][:-1]
        data_meta = data_meta.split(';')

        # delete hc and/or avh from data meta
        data_meta = [each for each in data_meta if not (each.startswith('hc') or each.startswith('avh'))]

        # make a dict
        data_meta_dict = {each.split('=')[0]: each.split('=')[1] for each in data_meta[:-1]}

        # Divide av to av+ and av-
        try:
            data_meta_dict['av+'] = data_meta[-1].split('=')[1].split(',')[0]
            data_meta_dict['av-'] = data_meta[-1].split('=')[1].split(',')[1]
        except Exception:
            data_meta_dict['r'] = None
            data_meta_dict['av+'] = None
            data_meta_dict['av-'] = None

        return data_meta_dict


class StoryCommentsParser:
    """
    It's a class that gets all the comments from a story from pikabu.ru by the story's id.
    """

    def __init__(self, story_id):
        self.story_id = story_id
        self.comments = self.proceed_get_story_comments_request()['data']

        print(f'We got {len(self.comments)} comments in story No {self.story_id}')

        ids = [each.id for each in self.comments]
        print('Ids: ', *ids)

    @staticmethod
    def set_anti_ddos_headers():
        user_agent_list = ['Chrome/108.0.0.0',
                           'Mozilla/5.0',
                           'Dalvik/2.1.0',
                           ]
        return {'User-Agent': random.choice(user_agent_list)}

    async def get_children(self, root_comments):
        """
        Gets all the child comments from the root comments list (Comment object)
        :param root_comments:
        :return: a dict with an error code (0 if all good), some message if needed and a list of comments
        """
        dict_to_return = {'error_code': 0,
                          'message': '',
                          'data': []
                          }
        tasks = [self.make_php_request(action='get_comments_subtree', id=comment.id) for comment in root_comments]

        start = time.time()
        child_comments = await asyncio.gather(*tasks)
        # Todo if make_php_request returns more than 300 comments start another parsing

        print(time.time() - start)

        # make a request trying to get 300 comments.
        return child_comments

    async def make_php_request(self, action, **kwargs):
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
            data_to_return = {'html comments': result_in_json['data']['html']}

        elif action == 'get_story_comments':
            data_to_return = {'total_number_of_comments': result_in_json['data']['total'],
                              'list_of_comments': result_in_json['data']['comments'],
                              }
        return data_to_return

    def get_root_comments(self) -> dict:
        """
        Gets all the root comments from the post. Completely ignores indented comments.
        :return: a dict with an error code (0 if all good), some message if needed and a list of comments (sorted
        as they go on the website).
        {
            'error_code': 0,
            'message': '',
            'data': list of comments,
        }
        """
        dict_to_return = {'error_code': 0,
                          'message': '',
                          'data': []
                          }
        # First request without conditions
        comment_id_to_start_searching = ''
        number_of_comments_in_response = PIKABU_DB_LIMIT

        while number_of_comments_in_response == PIKABU_DB_LIMIT:
            # Make a request
            result = asyncio.run(self.make_php_request(action='get_story_comments',
                                                       start_comment_id=comment_id_to_start_searching))
            total_number_of_comments = result['total_number_of_comments']

            # Quit if post has no comments
            if total_number_of_comments == 0:
                dict_to_return['message'] = 'There are no comments in the post'
                return dict_to_return

            list_of_comments = [Comment(each) for each in result['list_of_comments']]
            # result['list_of_comments']

            dict_to_return['data'].extend(list_of_comments)

            # Quit if post has few comments, and we got them all by one request
            if total_number_of_comments == len(list_of_comments):
                dict_to_return['message'] = 'The post was small and had a couple of root comments only'
                return dict_to_return

            # The id of the last root comment in the list
            comment_id_to_start_searching = dict_to_return['data'][-1].id
            number_of_comments_in_response = len(list_of_comments)

        dict_to_return['message'] = f"We have got {len(dict_to_return['data'])} root comments"
        return dict_to_return

    def proceed_get_story_comments_request(self):
        dict_to_return = {'error_code': 0,
                          'message': '',
                          'data': []
                          }
        # Get all the root comments from the post
        root_comments = self.get_root_comments()['data']

        comments_posts = [root_comment for root_comment in root_comments if root_comment.is_post()]
        comments_with_children = [root_comment for root_comment in root_comments if root_comment.has_children()]
        comments_without_children = [root_comment for root_comment in root_comments
                                     if not root_comment.has_children() and root_comment not in comments_posts]

        children = asyncio.run(self.get_children(comments_with_children))
        html_children_comments_list = []
        for big_html_of_children in children:
            soup = BeautifulSoup(big_html_of_children['html comments'], 'lxml')
            each_comment_html_list = [each.prettify() for each in soup.findAll('div', class_='comment')]
            html_children_comments_list.extend(each_comment_html_list)

        list_of_children_Comments = [self.convert_html_comment_to_Comment(each) for each in html_children_comments_list]

        dict_to_return['data'] += comments_with_children
        dict_to_return['data'] += list_of_children_Comments

        dict_to_return['data'] += comments_without_children
        dict_to_return['data'] += comments_posts



        return dict_to_return

    @staticmethod
    def convert_html_comment_to_Comment(html_comment: str):
        """
        Converts prettified by bs html comment to a Comment object
        """

        soup = BeautifulSoup(html_comment, 'lxml')
        # id
        com_id = int(soup.find('div', class_='comment').get('data-id'))

        # parent_id
        data_meta = soup.find('div', class_='comment').get('data-meta').split(';')
        parent_id = int(data_meta[0].split('=')[1])

        # html
        # If comment has children, they could appear in the comment's html. So lets delete the children from there
        for div in soup.findAll('div', class_='comment__children'):
            div.extract()

        # Save html without children
        html = soup.prettify()

        parsed_comment = {'id': com_id,
                          'parent_id': parent_id,
                          'html': html,
                          'is_hidden': None,
                          'is_hidden_children': None
                          }
        return Comment(parsed_comment)

if __name__ == '__main__':
    #a = StoryCommentsParser(story_id=10085566)  # https://pikabu.ru/story/biznes_ideya_10085566#comments 1900 comments
    a = StoryCommentsParser(story_id=10161553) #492 comments
    #a = StoryCommentsParser(story_id=5_555_555) #10 comments
    # a = StoryCommentsParser(story_id=6740346) #4000 comments badcomedian

    #a = StoryCommentsParser(story_id=10182975) #https://pikabu.ru/story/otzyiv_o_bmw_x6_10182975


