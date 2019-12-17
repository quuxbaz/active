#lang racket

(require db
         gregor
         deferred
         (prefix-in api: "api.rkt")
         "util.rkt")
(require racket/exn)

;; configuration
(define the-database (make-parameter "./the-database.db"))
(define *the-delay-interval* 60) ;; minutes

(define db-conn #f)

(define *db* (box #f))
(define (in-memory-db-set! db)
  (set-box! *db* db)
  (unbox *db*))

(define (in-memory-db-get [refresh? #f])
  ;;(displayln (current-directory))
  (if (file-exists? "./conf/cache") 
      (if refresh? 
          (begin (displayln "database: force refresh") 
                 (in-memory-db-set! (get-programs-and-activities-all)))
          (let ([in-mem (unbox *db*)]) 
            (if in-mem 
                (begin (displayln "database: cache hit") 
                       in-mem)
                (begin (displayln "database: cache miss") 
                       (in-memory-db-set! (get-programs-and-activities-all))))))
      (begin (displayln "database: cache is disabled") 
             (in-memory-db-set! (get-programs-and-activities-all)))))

(define (database-start-up)
  (set! db-conn
        (virtual-connection
         (connection-pool
          (lambda () 
            (get-connection))))))

(define (get-departments-all)
  (define sql "select distinct p_department from programs order by p_department")
  (sql-fetch sql))

(define (get-program-details pid)
  (define sql "select date(programs.p_updated_date) as p_updated_date_short, programs.* from programs where programs.p_id = $1")
  (define h (first (sql-fetch sql pid)))
  (hash-set! h 'sessions (get-program-sessions pid))
  h)

(define (get-program-sessions pid)
  (define sql "select sessions.* from sessions where sessions.p_id = $1")
  (sql-fetch sql pid))

(define (get-categories-all)
  (define sql "select distinct p_category from programs order by p_category")
  (sql-fetch sql))

(define (get-subcategories-all)
  (define sql "select distinct p_subcategory from programs order by p_subcategory")
  (sql-fetch sql))

(define (get-programs-and-activities-all)
  (define sql
    (string-append 
     "select * from programs where 1 = 1 and p_status = 'Open' "
     (if (file-exists? "./conf/hide-on-internet") " and p_hide_on_internet <> 'True' " " ")
     "order by p_name"))
  (printf "database: ~a~n" sql)
  (sql-fetch sql))

(define (get-programs-all)
  (define sql ")select * from programs where substr(p_id,1,1) = 'p' order by p_name")
  (sql-fetch sql))

(define (get-program-types-all)
  (define sql "select * from program_types order by ptype_name")
  (sql-fetch sql))

(define (get-sites-all)
  (define sql "select distinct p_site_name from programs order by p_site_name")
  (sql-fetch sql))

(define (get-activities-all)
  (define sql "select * from programs where substr(p_id,1,1) = 'a' order by p_name")
  (sql-fetch sql))

;; rows-result -> assoc-list
;; Makes a list of assoc-lists.  Each element of this list is a movie.
;; In each movie, you'll find its values, which are assoc-lists.
(define (sql-fetch . args)
  (define rr (apply query (cons db-conn args)))
  (rows-result->assoc rr))

(define (get-update-logs)
  (define path "logs")
  (for/list ([f (directory-list path)]
             #:when (and (file-exists? (build-path path f))
                         (regexp-match #rx"\\.log$" f)))
    (path->string f)))

(define (update [log? true])
  (printf "Update procedure begins...")
  (after *the-delay-interval* #:minutes (update log?))
  (with-handlers 
      ([exn? (lambda (e) 
               (displayln (exn->string e))
               (displayln (format "Update failed.  We'll run again in ~a minutes." *the-delay-interval*)))])
      ;; Let's protect the entire update against failure.  If anything
      ;; goes wrong here, we stop, but we let the next scheduled
      ;; update run.  The idea is that we would have to stop perhaps
      ;; because of API-server-failures.  In this case, we just try
      ;; again *the-delay-interval* minutes later.  Notice that
      ;; soft-failures such as a non-important API requests is handled
      ;; individually inside of deeper-level function calls in here.
      
      ;; Notice we're not protecting ourselves against output-port
      ;; redirections here.  We should adjust that eventually.  Racket
      ;; offers custodians and whatnot to protect ourselves against
      ;; that.
    (delete-old-files "logs/")
    (delete-old-files "tmp/")
    (define old-port (current-output-port))
    (when log?
      (current-output-port 
       (open-output-file 
        #:exists 'truncate 
        (string->path (~t (now) "'logs/update-'yyyy-MM-dd-HH-mm-ss'.log'")))))
    (file-stream-buffer-mode (current-output-port) 'line)
    (printf "Update @ ~a.~n" (~t (now) "yyyy-MM-dd HH:mm:ss"))
    (api:api-n-requests 0)
    (populate)
    (printf "The update procedure issued ~a api calls.~n" (api:api-n-requests))
    ;; I do not need to reset the api-n-requests counter.
    (in-memory-db-set! (get-programs-and-activities-all))
    (printf "Database refreshed.~n")
    (printf "Update completely done.~n")
    (when log?
      (close-output-port (current-output-port)))
    (current-output-port old-port)
    (printf "Update procedure just finished @ ~a.~n" (~t (now) "yyyy-MM-dd HH:mm:ss"))))

(define (schedule-update)
  (displayln "Scheduling updates...")
  (after *the-delay-interval* #:minutes (update)))

(define (get-connection)
  (displayln (format "get-connection: using database ~a." (the-database)))
  (sqlite3-connect #:database (the-database) #:mode 'create))

(define (table-exists? tbl)
  (define sql "select name from sqlite_master where name=$1")
  (query-maybe-value db-conn sql tbl))

;; setting up for the first time
(define (create-table-sessions)
  (define stm "CREATE TABLE if not exists sessions  (
                                                     p_id                INTEGER NOT NULL,
                                                     sess_id             INTEGER NOT NULL,
                                                     sess_name           TEXT,
                                                     sess_description    TEXT,
                                                     fac_name            TEXT,
                                                     fac_id              TEXT,
                                                     center_name         TEXT,
                                                     center_id           TEXT,
                                                     sess_min_enrollment INTEGER,
                                                     sess_max_enrollment INTEGER,
                                                     sess_enrollments    INTEGER,
                                                     sess_first_date     TEXT,
                                                     sess_last_date      TEXT,
                                                     sess_week_of_month  TEXT,
                                                     sess_days_of_week   TEXT,
                                                     sess_beginning_time TEXT,
                                                     sess_ending_time    TEXT)")
  (query-exec db-conn stm))

(define (create-table-programs)
  (define stm "CREATE TABLE if not exists programs (
                                                     p_id                 TEXT    PRIMARY KEY,
                                                     p_name               TEXT,
                                                     p_number             TEXT,
                                                     p_modified_date_time TEXT,
                                                     p_type               TEXT,
                                                     p_parent_season      TEXT,
                                                     p_child_season       TEXT,
                                                     p_status             TEXT,
                                                     p_hide_on_internet   TEXT,
                                                     p_department         TEXT,
                                                     p_category           TEXT,
                                                     p_subcategory        TEXT,
                                                     p_site_name          TEXT,
                                                     p_gender             TEXT,
                                                     p_age_min_year       INTEGER,
                                                     p_age_min_month      INTEGER,
                                                     p_age_min_week       INTEGER,
                                                     p_age_max_year       INTEGER,
                                                     p_max_month          INTEGER,
                                                     p_age_max_week       INTEGER,
                                                     p_updated_date       TEXT,
                                                     p_supervisor         TEXT,
                                                     p_description        TEXT,
                                                     p_tax_receipt        TEXT,
                                                     p_enrollments        INTEGER,
                                                     p_open_spaces        TEXT,
                                                     p_start_date         TEXT,
                                                     p_end_date           TEXT,
                                                     p_allow_waiting_list TEXT,
                                                     p_public_url         TEXT,
                                                     act_days             TEXT,
                                                     act_start_date       TEXT,
                                                     act_frequency        TEXT,
                                                     act_start_time       TEXT,
                                                     act_end_time         TEXT,
                                                     act_end_date         TEXT,
                                                     act_notes            TEXT,
                                                     p_last_updated       DATETIME)")
  (query-exec db-conn stm))

(define (create-table-program-types)
  (define stm "CREATE TABLE if not exists program_types (
                                                     ptype_id   INTEGER PRIMARY KEY,
                                                     ptype_name TEXT)")
  (query-exec db-conn stm))

(define (create-table-sites)
  (define stm "CREATE TABLE if not exists sites (
                                                     site_id              INTEGER PRIMARY KEY NOT NULL,
                                                     site_name            TEXT,
                                                     site_address1        TEXT,
                                                     site_address2        TEXT,
                                                     site_city            TEXT,
                                                     site_state           TEXT,
                                                     site_zip_code        TEXT,
                                                     site_geographic_area TEXT,
                                                     site_country         TEXT,
                                                     site_email_address   TEXT)")
  (query-exec db-conn stm))

(define (create-table-activities)
  (define stm
    "create table if not exists activities
     (act_id integer PRIMARY KEY not null
      ,act_number integer
      ,act_name text
      ,act_parent_season text
      ,act_child_season text
      ,act_status text
      ,act_type text
      ,act_department text
      ,category text
      ,act_other_category text
      ,site_name text )")
  (query-exec db-conn stm))

(define (set-up)
  (printf "Preparing database...~n")
  ;(update false)
  'done)

(define (set-up-later)
  (printf "Will update after ~a minutes...~n" *the-delay-interval*)
  (after *the-delay-interval* #:minutes #t)
  'done)

(define (populate)
  (displayln 
   (time
    (query-exec db-conn "update programs set p_last_updated = ''")
    (populate-table-programs-with-programs)
    (populate-table-programs-with-activities)
    (query-exec db-conn "delete from programs where length(p_last_updated) = 0")))
  'done)

(define fields-programs '(
    p_id
    p_name
    p_number            
    p_modified_date_time
    p_type              
    p_parent_season     
    p_child_season      
    p_status            
    p_hide_on_internet
    p_department        
    p_category          
    p_subcategory       
    p_site_name         
    p_gender            
    p_age_min_year
    p_age_min_month      
    p_age_min_week       
    p_age_max_year       
    p_max_month          
    p_age_max_week
    p_supervisor
    p_description
    p_tax_receipt
    p_enrollments
    p_open_spaces
    p_start_date
    p_end_date
    p_allow_waiting_list
    p_public_url
    act_days 
    act_frequency 
    act_start_time 
    act_end_time 
    act_notes))

(define fields-sessions '(
    p_id
    sess_id
    sess_name
    sess_description
    fac_name
    fac_id
    center_name
    center_id
    sess_min_enrollment
    sess_max_enrollment
    sess_enrollments
    sess_first_date
    sess_last_date
    sess_week_of_month
    sess_days_of_week
    sess_beginning_time
    sess_ending_time))

;; I'll need a little macro hash.refs from util.rkt.  (It's an
;; experiment.  The macro seems to shine in this case because we have
;; more than a couple of levels of JSON nesting here.)

(define (make-dollars n)
  (string-join 
   (build-list n
               (lambda (x) (format "$~a" (number->string (add1 x))))) ","))

(define (sessions-insert-list-of pid ls-of-sessions)
  (query-exec db-conn "delete from sessions where p_id = $1" pid)
  (displayln (format "Adding ~a sessions for program id = ~a" (length ls-of-sessions) pid))
  (define fields (string-join (map symbol->string fields-sessions) ","))
  (define stm 
    (format "INSERT INTO sessions (~a) VALUES (~a)" 
            fields (make-dollars (length fields-sessions))))
  (for ([s (in-list ls-of-sessions)])
    (printf "Session ~a ~a.~n" (hash.refs s.session_id) (hash.refs s.session_name))
    (query-exec db-conn stm 
                pid
                (hash.refs s.session_id)
                (hash.refs s.session_name)
                (hash.refs s.session_description)
                (hash.refs s.facility_name)
                (hash.refs s.facility_id)
                (hash.refs s.center_name)
                (hash.refs s.center_id)
                (hash.refs s.min_enrollment)
                (hash.refs s.max_enrollment)
                (hash.refs s.number_of_enrollment)
                (hash.refs s.first_date)
                (hash.refs s.last_date)
                (hash.refs s.week_of_month)
                (hash.refs s.days_of_week)
                (hash.refs s.beginning_time)
                (hash.refs s.ending_time))))

(define (update-program-details pid x)
  (display (format "Updating program detail for p_id = ~a... " pid))
  (define stm "UPDATE programs SET 
     p_updated_date = $1
   , p_supervisor = $2
   , p_description = $3
   , p_tax_receipt = $4
   , p_enrollments = $5
   , p_open_spaces = $6
   , p_start_date = $7
   , p_end_date = $8
   , p_allow_waiting_list = $9
   , p_public_url = $10
   , p_last_updated = datetime('now')
   WHERE p_id = $11")
  (query-exec db-conn stm     
    (hash.refs x.updated_date)
    (hash.refs x.supervisor)
    (hash.refs x.catalog_description)
    (hash.refs x.tax_receipt_eligibility)
    (hash.refs x.number_of_enrollments)
    (hash.refs x.open_spaces)
    (hash.refs x.start_date)
    (hash.refs x.end_date)
    (hash.refs x.allow_waiting_list)
    (hash.refs x.public_url)
    pid)
  (printf "done.~n"))

(define (update-activity-details pid x)
  (printf "Updating activity detail for p_id = ~a... " pid)
  (define stm "UPDATE programs SET 
     p_supervisor = $1
   , p_description = $2
   , p_tax_receipt = $3
   , p_enrollments = $4
   , p_open_spaces = $5
   , p_start_date = $6
   , p_end_date = $7
   , p_allow_waiting_list = $8
   , p_public_url = $9
   , act_days = $10
   , act_frequency = $11
   , act_start_time = $12
   , act_end_time = $13
   , act_notes = $14
   , p_last_updated = datetime('now')
   WHERE p_id = $15")
  (define ar (first ;; ar means ``activity recurrences''
              (let ([list-of-recurrences (hash.refs x.activity_recurrences)])
                (cond [(empty? list-of-recurrences) (list (make-hash))]
                      [else list-of-recurrences]))))
  (query-exec db-conn stm     
    (hash.refs x.supervisor)
    (hash.refs x.catalog_description)
    (hash.refs x.tax_receipt_eligibility)
    (hash.refs x.number_of_enrolled)
    (hash.refs x.open_spaces)
    (hash.refs x.default_beginning_date "")
    (hash.refs x.default_ending_date "")
    (hash.refs x.allow_waiting_list)
    (hash.refs x.public_url)
    (hash.refs ar.days "")
    (hash.refs ar.frequency "")
    (hash.refs ar.activity_start_time "")
    (hash.refs ar.activity_end_time "")
    (hash.refs x.receipt_notes)
    (format "~a" pid))
  (printf "done.~n"))

(define (populate-table-programs-with-programs)
  (define fields (string-join (map symbol->string fields-programs) ","))
  (displayln "Populating programs with programs...")
  (define stm (format "INSERT INTO programs (~a) VALUES (~a)" 
                      fields (make-dollars (length fields-programs))))
  ;; Strategy. First download all data to complete the record.  Then
  ;; do an atomic insert, after deleting the record.
  (with-handlers 
      ([list? (λ (e) 
                (displayln "caught exception while trying to update a program:") 
                (displayln e))])
    (for ([x (in-list (api:fetch-programs))])
      (let ([y (api:fetch-program-details (hash.refs x.program_id))]
            [pid (format "p~a" (hash.refs x.program_id))])
        (printf (format "Inserting program ~a ~a... " (hash.refs x.program_id) (hash.refs x.program_name)))
        (query-exec db-conn "delete from programs where p_id = $1" (format "p~a" (hash.refs x.program_id)))
        (query-exec db-conn stm 
                    pid
                    (hash.refs x.program_name)
                    (hash.refs x.program_number) 
                    (hash.refs x.modified_date_time)
                    (hash.refs x.program_type)
                    (hash.refs x.parent_season) 
                    (hash.refs x.child_season) 
                    (hash.refs x.program_status)
                    (hash.refs y.hide_on_internet)
                    (hash.refs x.program_department)
                    (hash.refs x.category)
                    (hash.refs x.sub_category)
                    (hash.refs x.site_name)
                    (hash.refs x.gender)
                    (hash.refs x.age_min_year)
                    (hash.refs x.age_min_month)
                    (hash.refs x.age_min_week)
                    (hash.refs x.age_max_year)
                    (hash.refs x.age_max_month)
                    (hash.refs x.age_max_week)
                    (hash.refs y.supervisor)
                    (hash.refs y.catalog_description)
                    (hash.refs y.tax_receipt_eligibility)
                    (hash.refs y.number_of_enrollments)
                    (hash.refs y.open_spaces)
                    (hash.refs y.start_date)
                    (hash.refs y.end_date)
                    (hash.refs y.allow_waiting_list)
                    (hash.refs y.public_url)
                    "" "" "" "" "" #|these last strings are activity-only|#)
        (printf "done.~n")
        (sessions-insert-list-of pid (hash.refs y.sessions '()))))
    'done))

(define (populate-table-programs-with-activities)
  (define fields (string-join (map symbol->string fields-programs) ","))
  (displayln "Populating programs with activities...")
  (define stm (format "INSERT INTO programs (~a) VALUES (~a)" 
                      fields (make-dollars (length fields-programs))))
  (with-handlers 
      ([list? (λ (e) 
                (displayln "caught exception while trying to update an activity:") 
                (displayln e))])
    (for ([x (in-list (api:fetch-activities))])
      (let* ([y (api:fetch-activity-details (hash.refs x.activity_id))]
             [pid (format "a~a" (hash.refs x.activity_id))]
             [ar (first ;; ar means ``activity recurrences''
                  (let ([list-of-recurrences (hash.refs y.activity_recurrences)])
                    (cond [(empty? list-of-recurrences) (list (make-hash))]
                          [else list-of-recurrences])))])
        (printf (format "Inserting activity ~a ~a... " (hash.refs x.activity_id) (hash.refs x.activity_name)))
        (query-exec db-conn "delete from programs where p_id = $1" (format "a~a" (hash.refs x.activity_id)))
        (query-exec db-conn stm 
                    pid
                    (hash.refs x.activity_name)
                    (hash.refs x.activity_number) 
                    (hash.refs x.modified_date_time)
                    (hash.refs x.activity_type)
                    (hash.refs x.parent_season) 
                    (hash.refs x.child_season) 
                    (hash.refs x.activity_status)
                    (hash.refs y.hide_on_internet)
                    (hash.refs x.activity_department)
                    (hash.refs x.category)
                    (hash.refs x.other_category)
                    (hash.refs x.site_name)
                    (hash.refs x.gender)
                    (hash.refs x.age_min_year)
                    (hash.refs x.age_min_month)
                    (hash.refs x.age_min_week)
                    (hash.refs x.age_max_year)
                    (hash.refs x.age_max_month)
                    (hash.refs x.age_max_week)
                    (hash.refs y.supervisor)
                    (hash.refs y.catalog_description)
                    (hash.refs y.tax_receipt_eligibility)
                    (hash.refs y.number_of_enrolled)
                    (hash.refs y.open_spaces)
                    (hash.refs y.default_beginning_date "")
                    (hash.refs y.default_ending_date "")
                    (hash.refs y.allow_waiting_list)
                    (hash.refs y.public_url)
                    (hash.refs ar.days "")
                    (hash.refs ar.frequency "")
                    (hash.refs ar.activity_start_time "")
                    (hash.refs ar.activity_end_time "")
                    (hash.refs y.receipt_notes))
        (printf "done.~n"))))
  'done)

(define (drop-table t)
  (query-exec db-conn (string-append "drop table if exists " t)))

(define (drop-all-tables)
  (for ([t '("programs" "sessions")])
    (drop-table t)))

(define (create-all-tables)
  (create-table-programs)
  (create-table-sessions)
)

;; rows-result -> list-of column
(define (columns rr)
  (for/list ([alist rr])
    (cdr (first alist))))

;; rows-result -> assoc
(define (rows-result->assoc rr)
  (define (grouped->header blob)
    (cdr (first (last blob))))
  (define names-and-types
    (for/list ([header (rows-result-headers rr)]) 
      (cons (cdar header) (cdadr header))))
   (for/list ([vec (rows-result-rows rr)])
     (begin 
       (define h (make-hash))
       (for ([header names-and-types]
             [val vec])
         (if (string=? (car header) "grouped")
             (hash-set! h (string->symbol (grouped->header header)) (sql-null->false val))
          (hash-set! h (string->symbol (car header)) (sql-null->false val))))
       h)))

;; key assoc -> value
(define (assoc-val key alist)
  (last (assoc key alist)))

(define (session-exists? sess-id)
  (define stm "SELECT sess_id from sessions where sess_id = $1")
  (query-maybe-value db-conn stm sess-id))

(define (activity-exists? act-id)
  (define stm "SELECT act_id from activities where act_id = $1")
  (query-maybe-value db-conn stm act-id))

(define (datetime->sql-datetime-string gregor-dt)
  (~t gregor-dt "yyyy-MM-dd HH:mm:SS"))

(define repl-cwd-done (make-parameter #f))
(define (repl)
  (when (not (repl-cwd-done))
  (current-directory "")
    (repl-cwd-done #t))
  (api:api-key "")
  (api:api-username "")
  (database-start-up))

(provide (all-defined-out))
