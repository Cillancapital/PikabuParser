from bs4 import BeautifulSoup


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

        self.content_tag_html = self.soup.find(class_='comment__content').prettify()
        self._clean_soup()

        self.metadata: dict = self._get_data_meta_from_soup()
        self.author: dict = self._get_author()
        self.id: int = int(self.soup.find('div', class_='comment').get('data-id'))
        self.parent_id: int = self.metadata['pid']

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
        r=1294;                            - total rating of the comment
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
