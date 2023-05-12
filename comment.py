from bs4 import BeautifulSoup


class Comment:
    """
    Represents a comment and contains all the useful information of the comment.
    To create an instance, it takes a parsed comment in HTML format as a string.

    Comment object has the following attributes:
        - content: BS object. Serves for the further parsing.
        - metadata: is a dict with following information:
            - id: Comment ID
            - author: Nickname of the author of the comment
            - pid: The parent ID (0 if no parent)
            - aid: ID of the author of the comment
            - sid: ID of the story where the comment was published
            - said: ID of the author of the story where the comment was published
            - d: Date the comment was published in the format '2023-03-28T17:41:30+03:00'
            - de: Unknown definition
            - ic: Unknown definition
            - r: Total rating of the comment
            - av+: Votes in favor
            - av-: Votes against
        - url_post_comment: URL of the post if the comment is a post, otherwise an empty string
        - id_post_comment: ID of the post if the comment is a post, otherwise 0
    """

    def __init__(self, parsed_comment: str):
        self.soup = BeautifulSoup(parsed_comment, 'lxml')
        self.content: BeautifulSoup = self._get_content()
        self.metadata: dict = self._get_data_meta_from_soup()
        self.url_post_comment: str = self._is_post()
        self.id_post_comment: int = self._get_id_post_comment()
        del self.soup

    def _get_content(self) -> BeautifulSoup:
        """
        Extracts and returns the BS object of the comment content.

        Returns:
            str: The BeautifulSoup object of the comment content.
        """
        content_html_tag = self.soup.find(class_='comment__content')
        content_html_tag.extract()
        return content_html_tag

    def _get_data_meta_from_soup(self) -> dict:
        """
        Retrieves and returns all metadata from the HTML.

        Returns:
            dict: A dictionary containing the extracted metadata.
        """

        data = {'id': int(self.soup.find(class_='comment').get('data-id')),
                'author': self._get_author()}

        # Raw data-meta is a string 'pid=0;aid=3296271;sid=10085566;said=4874925;...'
        data_meta: str = self.soup.find(class_='comment').get('data-meta')
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

        data.update(data_meta)

        return data

    def _get_author(self) -> str:
        """
        Retrieves and returns the author nickname of the comment.

        Returns:
            str: The author's nickname of the comment.
        """
        user_tag = self.soup.find(class_='comment__user')
        return user_tag.get('data-name')

    def _is_post(self) -> str:
        """
        Checks if the comment is a post based on its HTML.

        Returns:
            str: The post URL if the comment is a post, otherwise an empty string.
        """
        post = self.soup.find(class_='comment_comstory')
        return post.get('data-url') if post else ''

    def _get_id_post_comment(self) -> int:
        """
        Retrieves and returns the post ID of the comment if it is a post.

        Returns:
            int: The post ID if the comment is a post, otherwise 0.
        """
        if self.url_post_comment:
            id_post = int(self.url_post_comment[self.url_post_comment.rfind('_') + 1:])
        else:
            id_post = 0
        return id_post
