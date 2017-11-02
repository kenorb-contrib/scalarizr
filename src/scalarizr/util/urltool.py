from __future__ import with_statement
'''
Created on May 23, 2011

@author: marat
'''

import urllib2

class HTTPRedirectHandler(urllib2.HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Return a Request or None in response to a redirect.
        This is called by the http_error_30x methods when a
        redirection response is received.  If a redirection should
        take place, return a new Request to allow http_error_30x to
        perform the redirect.  Otherwise, raise HTTPError if no-one
        else should try to handle this url.  Return None if you can't
        but another Handler might.
        """

        m = req.get_method()
        if (code in (301, 302, 303, 307) and m in ("GET", "HEAD")
                or code in (301, 302, 303, 305) and m == "POST"):
            # Strictly (according to RFC 2616), 301 or 302 in response
            # to a POST MUST NOT cause a redirection without confirmation
            # from the user (of urllib2, in this case).  In practice,
            # essentially all clients do redirect in this case, so we
            # do the same.
            # be conciliant with URIs containing a space
            newurl = newurl.replace(' ', '%20')
            newheaders = dict((k, v) 
                for k, v in req.headers.items()
                if k.lower() not in ("content-length", "content-type"))
            return urllib2.Request(newurl,
                                    data=req.get_data(),
                                    headers=newheaders,
                                    origin_req_host=req.get_origin_req_host(),
                                    unverifiable=True)
        else:
            raise urllib2.HTTPError(req.get_full_url(), code, msg, headers, fp)

    http_error_305 = urllib2.HTTPRedirectHandler.http_error_302
