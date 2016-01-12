/**
 * Static factory class for building codesearch ES filter objects.
 */

package com.palantir.stash.codesearch.search;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.orQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.typeQuery;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;

import com.atlassian.bitbucket.repository.Repository;
import com.google.common.collect.Iterators;
import com.palantir.stash.codesearch.logger.PluginLoggerFactory;

public class SearchFilterUtils {

    private final Logger log;

    public SearchFilterUtils(PluginLoggerFactory plf) {
        this.log = plf.getLogger(this.getClass().toString());
    }

    private <T> Iterable<T> toIterable(final T[] array) {
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return Iterators.forArray(array);
            }
        };
    }

    public QueryBuilder exactRefQuery(String ref) {
        return termQuery("refs.untouched", ref);
    }

    public QueryBuilder projectRepositoryQuery(String project, String repository) {
        return boolQuery()
            .filter(termQuery("project", project))
            .filter(termQuery("repository", repository));
    }

    public QueryBuilder aclQuery(Map<String, Repository> repoMap) {
        if (repoMap.isEmpty()) {
            return boolQuery().mustNot(matchAllQuery());
        }

        // Create disjunction of individual repo ACL filters
        BoolQueryBuilder query = boolQuery();
        for (Repository repo : repoMap.values()) {
            query.filter(projectRepositoryQuery(repo.getProject().getKey(), repo.getSlug()));
        }
        return query;
    }

    public QueryBuilder refQuery(String[] refs) {
        return refQuery(toIterable(refs));
    }

    public QueryBuilder refQuery(Iterable<String> refs) {
        boolean filterAdded = false;
        BoolQueryBuilder query = boolQuery();
        for (String ref : refs) {
            String[] toks = ref.split("[/\\s]+");
            // Make sure there's at least one non-empty token
            boolean emptyTokens = true;
            for (String tok : toks) {
                if (!tok.isEmpty()) {
                    emptyTokens = false;
                    break;
                }
            }
            if (emptyTokens) {
                continue;
            }

            BoolQueryBuilder refQuery = boolQuery();
            for (String tok : toks) {
                if (!tok.isEmpty()) {
                    refQuery.filter(termQuery("refs", tok.toLowerCase()));
                }
            }
            query.should(refQuery);
            filterAdded = true;
        }
        return filterAdded ? filter : matchAllFilter();
    }

    public QueryBuilder personalFilter() {
        BoolQueryBuilder filter = boolFilter();
        filter.mustNot(prefixFilter("project", "~")
                .cache(true));
        return filter;
    }

    public QueryBuilder projectFilter(String[] projects) {
        return projectFilter(toIterable(projects));
    }

    public QueryBuilder projectFilter(Iterable<String> projects) {
        boolean filterAdded = false;
        BoolQueryBuilder filter = boolFilter();
        for (String project : projects) {
            project = project.trim();
            if (project.isEmpty()) {
                continue;
            }
            filter.should(termFilter("project", project)
                .cache(true)
                .cacheKey("CACHE^PROJECTFILTER^" + project));
            filterAdded = true;
        }
        return filterAdded ? filter : matchAllFilter();
    }

    public QueryBuilder repositoryFilter(String[] repositories) {
        return repositoryFilter(toIterable(repositories));
    }

    public QueryBuilder repositoryFilter(Iterable<String> repositories) {
        boolean filterAdded = false;
        BoolQueryBuilder filter = boolFilter();
        for (String repository : repositories) {
            repository = repository.trim();
            if (repository.isEmpty()) {
                continue;
            }
            filter.should(termFilter("repository", repository)
                .cache(true)
                .cacheKey("CACHE^REPOFILTER^" + repository));
            filterAdded = true;
        }
        return filterAdded ? filter : matchAllFilter();
    }

    public QueryBuilder extensionFilter(String[] extensions) {
        return extensionFilter(toIterable(extensions));
    }

    public QueryBuilder extensionFilter(Iterable<String> extensions) {
        boolean filterAdded = false;
        BoolQueryBuilder filter = boolFilter();
        for (String extension : extensions) {
            extension = extension.trim();
            if (extension.isEmpty()) {
                continue;
            }
            filter.should(termFilter("extension", extension)
                .cache(true)
                .cacheKey("CACHE^EXTENSIONFILTER^" + extension));
            filterAdded = true;
        }
        return filterAdded ? filter.should(typeFilter("commit")) : matchAllFilter();
    }

    public QueryBuilder authorFilter(String[] authors) {
        return authorFilter(toIterable(authors));
    }

    public QueryBuilder authorFilter(Iterable<String> authors) {
        boolean filterAdded = false;
        BoolQueryBuilder filter = boolFilter();
        for (String author : authors) {
            String[] toks = author.split("\\W+");
            boolean emptyTokens = true;
            for (String tok : toks) {
                if (!tok.isEmpty()) {
                    emptyTokens = false;
                    break;
                }
            }
            if (emptyTokens) {
                continue;
            }

            // Name filters
            BoolQueryBuilder nameFilter = boolFilter();
            for (String tok : toks) {
                if (!tok.isEmpty()) {
                    nameFilter.must(termFilter("commit.authorname", tok.toLowerCase()));
                }
            }
            filter.should(nameFilter);

            // Email filters
            BoolQueryBuilder emailFilter = boolFilter();
            for (String tok : toks) {
                if (!tok.isEmpty()) {
                    emailFilter.must(termFilter("commit.authoremail", tok.toLowerCase()));
                }
            }
            filter.should(emailFilter);
            filterAdded = true;
        }
        return filterAdded ? filter.should(typeFilter("file")) : matchAllFilter();
    }

    public QueryBuilder dateRangeFilter(ReadableInstant from, ReadableInstant to) {
        if (from == null && to == null) {
            return matchAllFilter();
        }
        RangeQueryBuilder dateFilter = rangeFilter("commit.commitdate");
        if (from != null) {
            dateFilter.gte(from.getMillis());
        }
        if (to != null) {
            dateFilter.lte(to.getMillis());
        }
        // Match all files as well, since they don't have date info (user can turn off by
        // unchecking "search files" option.)
        return orFilter(dateFilter, typeFilter("file"));
    }

}
