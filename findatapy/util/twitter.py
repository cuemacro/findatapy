__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on a "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#

from twython import Twython

from findatapy.util import DataConstants, LoggerManager


class Twitter(object):
    """Has functions to tweet from Python (using Twython library)

    """

    def __init__(self, *args, **kwargs):
        self.logger = LoggerManager().getLogger(__name__)

    def set_key(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET):
        self.twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN,
                               OAUTH_TOKEN_SECRET)

    def auto_set_key(self):
        self.twitter = Twython(DataConstants().APP_KEY,
                               DataConstants().APP_SECRET,
                               DataConstants().OAUTH_TOKEN,
                               DataConstants().OAUTH_TOKEN_SECRET)

    def update_status(self, msg, link = None, picture = None):
        # 22 chars URL
        # 23 chars picture

        chars_lim = 140

        if link is not None: chars_lim = chars_lim - (22 * link)
        if picture is not None: chars_lim = chars_lim - 23

        if (len(msg) > chars_lim):
            self.logger.info("Message too long for Twitter!")

        if picture is None:
            self.twitter.update_status(status=msg)
        else:
            photo = open(picture, 'rb')
            self.twitter.update_status_with_media(status=msg, media=photo)




