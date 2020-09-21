SELECT
  template
FROM
  templates t, repositories r
WHERE
  (t.repo_id = r.repo_id AND main_language = 'java')
UNION
(SELECT
  template
FROM
  templates t, repositories r
WHERE
  (t.repo_id = r.repo_id AND main_language = 'c') AND random() < 0.5
LIMIT 3000)
;
