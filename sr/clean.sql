delete from pages_for_class where sitetext='TIMEDOUT' or sitetext='ERROR' or sitetext='MALFORMED_URI' or sitetext='PAGE REDIRECTS TO GODADDY' or sitetext='PAGE REDIRECTS TO BAD SITE';
create table pages_for_class_ts as select uid, class, website,to_tsvector(sitetext) as sitevec from pages_for_class;
