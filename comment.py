import asyncio

from bs4 import BeautifulSoup
import re


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

    def manage_content(self):
        """
        Retrieves text, pictures, gifs and videos links from self comment content soup.

        Return: ?
        """
        def get_text():
            def clean_text_from_youtube_marks(some_text):
                youtube_pattern = re.compile(r'YouTube●\d+:\d+')
                return re.sub(youtube_pattern, '', some_text)

            text = self.content.text.replace('\n', '').replace('\t', '').strip()
            if text:
                text = clean_text_from_youtube_marks(text)
                text = text.encode('utf-8').decode()
                return text
            else:
                return ''

        def get_pics():
            def delete_video_previews_and_gifs(inner_img_links_list):
                if not inner_img_links_list:
                    return []
                clean_list = [pic for pic in inner_img_links_list if
                              '.gif' not in pic and 'https://i.ytimg.com' not in pic]
                return clean_list if clean_list else []

            images_html = self.content.findAll('img')
            if images_html:
                # for old html structure
                img_links_list = [img.get('src') for img in images_html]
                if None not in img_links_list:
                    return delete_video_previews_and_gifs(img_links_list)
                else:
                    # for mew html structure
                    img_links_list = [img.get('data-src') for img in images_html]
                    return delete_video_previews_and_gifs(img_links_list)
            else:
                return []

        def get_gifs():
            gif_html = self.content.findAll(class_='player player_width_limit')
            gif_links_list = [gif.get('data-source') for gif in gif_html]
            return gif_links_list if gif_links_list else []

        def get_videos():
            videos_html = self.content.findAll(class_='comment-external-video__content')
            video_links_list = [video.get('data-external-link') for video in videos_html]
            return video_links_list if video_links_list else []

        def get_comment_link():
            return f"https://pikabu.ru/story/_{self.metadata['sid']}?cid={self.metadata['id']}"

        return get_text(), get_pics(), get_gifs(), get_videos(), get_comment_link()


if __name__ == '__main__':
    # testing
    import aiohttp
    import json

    async def request_one_comment_only(comment_ids):
        url = 'https://pikabu.ru/ajax/comments_actions.php'
        data = {'action': 'get_comments_by_ids',
                'ids': comment_ids}
        async with aiohttp.ClientSession() as session:
            async with session.post(url,
                                    data=data,
                                    params={'g': 'goog'},
                                    headers={'User-Agent': 'Chrome/108.0.0.0'},
                                    ) as response:
                result = await response.text()
        result_in_json = json.loads(result)
        return result_in_json['data'][0]['html']

    # 1 picture 273116029
    # 1 YouTube video 273584720
    # 1 pic and 1 gif 273546462
    ids = '273546462'
    html = asyncio.run(request_one_comment_only(ids))
    com = Comment(html)
    print(com.manage_content())

