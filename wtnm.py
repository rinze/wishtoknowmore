# -*- coding: utf-8 -*-

import praw
import datetime
import sqlite3
import requests
import json
import pymysql
import urllib2

from StringIO import StringIO
from collections import Counter
from lxml import html, etree

from config import *

class WTNM:
    """
    Utility class. Wraps necessary methods around a PRAW's Reddit r object.
    and a MySQL connection.
    
    Does bot things when asked.
    """
    def __init__(self, test = False):
        """
        Inits the internal Reddit access (r) and sets up the latest dates
        obtained from the database.
        """
        self.__version__ = '0.1beta'

        # AUTH
        r = praw.Reddit('IWishToKnowMore {version} by /u/RinzeWind'.format(version = self.__version__))
        r.set_oauth_app_info(client_id, client_secret, redirect_url)
    
        r.set_access_credentials(**credentials)
    
        # Just some random stuff to check that we actually started the client
        authenticated_user = r.get_me()
        print "Here I am! I am {username} and I have {linkkarma} link karma. Ready to process!"\
                .format(username = authenticated_user.name, 
                        linkkarma = authenticated_user.link_karma)

        self.r = r
        self.__request_string__ = "!wishtoknowmore"
        self.con = pymysql.connect(host = mysql_host,
                                   user = mysql_user,
                                   password = mysql_pass,
                                   db = mysql_db,
                                   autocommit = True,
                                   charset = 'utf8')

        if test:
            self.__subreddit__ = 'IWTNMtest'
            r.submit(self.__subreddit__, 'Test ' + str(datetime.datetime.now()), 
                     text = "It works!")
            print "Test mode: article submitted to {}".format(self.__subreddit__)
        else:
            self.__subreddit__ = "WishToKnowMore"

    def classify_requests(self, requests):
        """
        Check whether the new batch of requests were already either processed
        or monitored. In that case, reply to the request with the proper
        information ("the request is already processed" / "waiting to be processed")
        and remove them from the array being returned, which will contain only
        those threads new to the system.
        """
        q = """SELECT m.request_thread,
                      m.request_reply,
                      p.post_id,
                      m.request_comment_id,
                      p.request_thread
               FROM monitor AS m  
               LEFT OUTER JOIN processed AS p ON m.request_thread = p.request_thread 
               WHERE m.request_thread IN ({thread_list}) OR p.request_thread IN ({thread_list})"""\
            .format(thread_list = ",".join(["'" + x['link_id'] + "'" for x in requests]))
        res = self._query(q)

        # Divide the list of requests in three groups:
        # 1. New. Will be returned.
        # 2. Already queued for processing. Will reply with a link to the
        #    original request.
        # 3. Already processed. Will reply with a link to the processed post.
        # 
        # Returned data will maintain the original request structure. already_queued
        # and already_processed requests will get an additional variable, 'reply_with'
        queued_threads = set([x[0] for x in res])
        processed_threads = set([x[4] for x in res if x[4] is not None])

        new_requests = list()
        already_queued = list()
        already_processed = list()

        for request in requests:
            thread_id = request['link_id']
            if thread_id not in queued_threads and thread_id not in processed_threads:
                new_requests.append(request)
            elif thread_id in queued_threads and thread_id not in processed_threads:
                res_line = [x for x in res if x[0] == thread_id][0] # only one
                request['reply_with'] = res_line[3]
                already_queued.append(request)
            else:
                res_line = [x for x in res if x[0] == thread_id][0] # only one
                request['reply_with'] = res_line[2]
                already_processed.append(request)

        return (new_requests, already_queued, already_processed)


    def get_new_requests(self):
        """
        Obtains latest requests from the comments.
        """
        last_observed = self._get_last_monitored_thread_comment()[0][0]
        print "Last observed thread: {}".format(last_observed)

        # Search reddit for the relevant strings
        results = self._search_comments(last_observed)

        # Remove results for which the comment is not only __request_string__
        # and return this list of dictionaries.
        results = [x for x in results if x['body'].lower() == self.__request_string__]

        return results

    def monitor_thread(self, thread):
        """
        Saves this thread into the DB for further monitoring. Also
        replies to it so the user knows we are already aware of the 
        request.
        """
        # Reply to comment
        print "Replying to request {id} by {user}".format(id = thread['id'],
                                                          user = thread['author'])
        comment = self.r.get_info(thing_id = "t1_" + thread['id'])
        
        # If the comment is deleted, just flag it as answered and return
        if comment.banned_by is not None or comment.author is None:
            self._update_last_comment("t1_" + thread['id'])
            return

        response = comment.reply("""Thanks! I have registered your request and
                                    it will appear in /r/{subreddit} shortly.
                                    Please remember that, in order to process a thread,
                                    I will wait 24 hours since its creation date. If the thread
                                    you are requesting me to summarise is older than that, it will be
                                    processed in the next batch."""\
                                 .format(subreddit = self.__subreddit__))
        values = [thread['created_utc'], thread['link_created_utc'],
                  "t1_" + thread['id'], thread['author'],
                  #None, 
                  "t1_" + response.id, 
                  thread['link_id']]
        self._query("""INSERT INTO monitor (request_timestamp,
                                            thread_creation_timestamp,
                                            request_comment_id,
                                            request_user,
                                            request_reply,
                                            request_thread)
                       VALUES (%s, %s, %s, %s, %s, %s)""", values)
        print "Request {id} added to monitoring queue".format(id = thread['id'])
        self._update_last_comment("t1_" + thread['id'])

    def process_pending(self):
        """
        Processes pending threads (monitored but still not processed). Posts in
        self.__subreddit__ and replies to the original requester.
        """

        pending_threads = self._query("""SELECT m.* FROM monitor m
                                         LEFT OUTER JOIN processed p ON p.request_thread = m.request_thread
                                         WHERE p.request_thread IS NULL
                                         AND m.thread_creation_timestamp < UNIX_TIMESTAMP(LOCALTIMESTAMP()) - (3600 * 24)
                                         ORDER BY m.thread_creation_timestamp""")
        if len(pending_threads) > 0:
            print "Processing {} monitored threads...".format(len(pending_threads))
        else:
            print "No threads are being monitored right now"

        for x in pending_threads:
            print "Processing thread {}... ".format(x[5])
            submission = self.r.get_info(thing_id = x[5])
            request_comment = self.r.get_info(thing_id = x[2])
            n_comments, links = self._get_thread_links(submission)

            if len(links) != 0:
            
                # Generate message
                s_title = submission.title
                if len(s_title) > 100:
                    s_title = s_title[:100]
                m_title = "Links for \"{}\" from /r/{}".format(s_title, submission.subreddit)
                if len(m_title) > 300:
                    m_title = m_title[:300]

                m_body = """This is the summary for the links found in the
                            comments for [this original submission]({orig_permalink}). I
                            inspected {n_comments} comments, from which {n_links} links were extracted. 
                            This below is a summary of all the links found in
                            the discussion thread.\n\n*****\n\n"""\
                         .format(orig_permalink = submission.permalink,
                                 n_comments = n_comments,
                                 n_links = len(links))
                # Table
                m_body += "score | link\n"
                m_body += "-----:|:----\n"
                for url in links:
                    row = "[{score}]({permalink}) | "\
                            .format(score = url['score'],
                                    permalink = url['permalink'])
                    if len(url['link']) > 50:
                        url_str = url['link'][:50] + "..."
                    else:
                        url_str = url['link']
                    row += "[{url_str}]({url})\n".format(url_str = url_str, url = url['link'])
                    m_body += row

                # Write the response and keep a reference to the submission
                # in order to a) inform the original requester; b) store that
                # info into the DB.
                s = self.r.submit(self.__subreddit__, m_title, text = m_body)
                if submission.over_18:
                    # Our post must be NSFW too
                    s.mark_as_nsfw()
                post_id = "t3_" + s.id

                # Generate reply text
                reply_text = """Hi again,\n\nI've generated a nice table with
                {n_links} links from this thread.\n Please go to [this
                post]({permalink}) to find it!""".format(n_links = len(links),
                                                         permalink = s.permalink)
                      
            else:
                # Inform the user that no links were found 
                reply_text = """Hi again,\n\nI am sorry to inform that this
                thread did not contain any link (or I couldn't find any!)"""
                post_id = None

            # Reply to original request
            request_comment.reply(reply_text)

            # Mark the thread as processed
            self._query("""INSERT INTO processed 
                           (processing_timestamp, post_id, request_thread)
                           VALUES (%s, %s, %s)""", 
                        [None, post_id, x[5]])

    def _get_thread_links(self, submission):
        """
        Get the top 100 links from the comments of a given thread.
        If a link is repeated, it will keep only the comment that
        has the highest score.
        """
        
        # This can be too much for the poor bot. Let's agree a simple rule here:
        # The "More comments" structure will be expanded IF there are at least
        # 10 replies below the fold. Otherwise, those comments will be skipped.
        # Changes that a high-scored link is there are small. We can live with
        # that (I can, at least).
        submission.replace_more_comments(limit = None, threshold = 10)
        flat_comments = praw.helpers.flatten_tree(submission.comments)
        n_comments = len(flat_comments)
        links = dict()
        for comment in flat_comments:
            # Process comments that have some link and have score greater
            # than one (that is, not only the submitter thought it was interesting).
            if comment.score > 1 and "<a href" in comment.body_html:
                tree = etree.parse(StringIO(comment.body_html))
                # Make an explicit copy of the list and delete the tree
                # to avoid memory problems
                l = list(tree.xpath('//a/@href'))
                del tree
                # Do not include references to other users or subreddits
                l = [x for x in l if not x.startswith('/') and not x.startswith('#')]
                if len(l) > 0: # don't do anything if there are no links left
                    for url in l:
                        if url not in links or links[url]['score'] < comment.score:
                            links[url] = dict(score = comment.score, 
                                              link = url,
                                              permalink = comment.permalink)

        # Conver to list and sort by score (descending)
        links = links.values()
        links.sort(key = lambda x: -x['score'])
        links = links[:50] # 50 links max
        return n_comments, links

    def reply_already_queued(self, request):
        print "Replying to request {id} by {user}".format(id = thread['id'],
                                                          user = thread['author'])
        comment = self.r.get_info(thing_id = "t1_" + request['id'])

        if comment.banned_by is not None or comment.author is None:
            self._update_last_comment("t1_" + thread['id'])
            return

        original_request = self.r.get_info(thing_id = request['reply_with'])
        comment.reply("""Thanks for your request! This thread is already stored for processing. [Here]({orig})
                         is the original request""".format(orig = original_request.permalink))
        self._update_last_comment("t1_" + request['id'])

    def reply_already_processed(self, request):
        print "Replying to request {id} by {user}".format(id = thread['id'],
                                                          user = thread['author'])
        comment = self.r.get_info(thing_id = "t1_" + request['id'])

        if comment.banned_by is not None or comment.author is None:
            self._update_last_comment("t1_" + thread['id'])
            return

        original_request = self.r.get_info(thing_id = request['reply_with'])
        comment.reply("""Thanks for your request! This thread has already been processed. [Here]({orig})
                         is the final link compilation.""".format(orig = original_request.permalink))
        self._update_last_comment("t1_" + request['id'])


    def _search_comments(self, last_comment = None):
        """
        Returns a list of the latest requests for threads to summarize.
        Only goes M minutes back in time.
        Uses https://www.reddit.com/r/redditdev/comments/3zug2y/a_tool_for_searching_reddit_comments_and/
        """
        url = "https://api.pushshift.io/reddit/search?q=%22{}%22"\
                .format(urllib2.quote(self.__request_string__))
        if last_comment:
            url += "&after_id=" + last_comment
        
        try:
            req = requests.request("GET", url)
            return json.loads(req.text)['data'] # don't need metadata
        except:
            # Probably a timeout. Return an empty list, will retrieve
            # everything later
            print "Couldn't get data from api.pushshift.io"
            return []

    def _update_last_comment(self, last_comment):
        """
        Because this table is very simple, we have to do two queries.
        No big problem
        """
        previous_last_comment = self._query("SELECT last_comment FROM last_comment")[0][0]
        if previous_last_comment < last_comment:
            q = "UPDATE last_comment SET last_comment = '%s'" % (last_comment,)
            self._query(q)
        
    def _get_last_monitored_thread_comment(self):
        q = "SELECT last_comment FROM last_comment"
        return self._query(q)
        
    def _query(self, q, values = None):
        """
        Interacts with the DB and returns the comment that triggered
        the last thread monitoring event.
        """
        with self.con.cursor() as cursor:
            cursor.execute(q, values)
            res = cursor.fetchall()
        return res
        
    def __del__(self):
        self.con.close()

if __name__ == "__main__":
    
    # Create bot instance on this Reddit connection
    wtnm = WTNM()

    # Process all pending requests
    sub = wtnm.process_pending()
    
    new_requests = wtnm.get_new_requests()

    if len(new_requests) == 0:
        print "No new requests."
    else:
        # Remove these requests that belong to a thread that is already
        # being monitored or processed
        new_requests, already_queued, already_processed = wtnm.classify_requests(new_requests)
        print "Number of total new requests:", len(new_requests)

        # What if there are more than one request for a given thread in new_requests?
        # Simply move the oldest to already_queued and process later. Process the first duplicate
        # immediately so we know the response id
        request_threads = [x['link_id'] for x in new_requests]
        counter = Counter(request_threads)
        for r in counter:
            if counter[r] > 1:
                dupes = []
                # Remove all from new_requests, will process apart
                for i in range(len(new_requests)):
                    if new_requests[i]['link_id'] == r:
                        dupes.append(new_requests[i])
                        new_requests[i] = None # Can's use .pop(i) as that changes the indexing
                new_requests = [x for x in new_requests if x is not None]
                # Sort by timestamp (possibly redundant, but doesn't hurt)
                dupes.sort(key = lambda x: x['created_utc'])
                # The first one is the one to process now, the rest will
                # simply receive the "already_queued" message with a link
                # to the first request.
                new_requests.append(dupes[0])
                for req in dupes[1:]:
                    req['reply_with'] = "t1_" + dupes[0]['id']
                    already_queued.append(req)

        # Now we have all the requests sorted in three different
        # categories. In order to maintain internal consistency,
        # it is very important to process them in the order they
        # were produced. In order to do that, we will use the 
        # 'id' field as a key to a dictionary per request type,
        # and will then process the elements in order
        new_requests_d = dict()
        already_queued_d = dict()
        already_processed_d = dict()
        ids = list()

        for r in new_requests:
            new_requests_d[r['id']] = r
            ids.append(r['id'])
        for r in already_queued:
            already_queued_d[r['id']] = r
            ids.append(r['id'])
        for r in already_processed:
            already_processed_d[r['id']] = r
            ids.append(r['id'])

        # So we process in strict order.
        ids.sort()

        print "{} new requests, {} already queued, {} already processed"\
               .format(len(new_requests), len(already_queued), len(already_processed))

        for i in ids:
            if i in new_requests_d:
                wtnm.monitor_thread(new_requests_d[i])
            elif i in already_queued_d:
                wtnm.reply_already_queued(already_queued_d[i])
            elif i in already_processed_d:
                wtnm.reply_already_processed(already_processed_d[i])
            else:
                raise "This should never happen"

    # Remove bot reference to make sure we correctly close all connections
    del wtnm
