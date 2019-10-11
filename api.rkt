#lang racket/base

(require 
 racket/file
 racket/list
 racket/string
 racket/port
 racket/bool
 net/http-client
 net/base64
 openssl/sha1
 json
 gregor
 "util.rkt")

;; globals
(define api-host "api.amp.active.com")
(define api-thing "anet-systemapi")
(define api-key (make-parameter #f))
(define api-username (make-parameter #f))
;; (define api-password (make-parameter #t))

(define (fetch-program-details pid)
  (define fmt "https://~a/~a/~a/api/v1/flexregprograms/~a?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) pid (api-key)))
  (first (map convert-values! (hash-ref (read-json (fetch-uri api-host uri)) 'body))))

(define (fetch-programs)
  (define fmt "https://~a/~a/~a/api/v1/flexregprograms?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) (api-key)))
  (fetch-all-pages-of uri))

(define (fetch-program-types)
  (define fmt "https://~a/~a/~a/api/v1/flexregprogramtypes?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) (api-key)))
  (map convert-values! (hash-ref (read-json (fetch-uri api-host uri)) 'body)))

(define (fetch-sites)
  (define fmt "https://~a/~a/~a/api/v1/sites?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) (api-key)))
  (map convert-values! (hash-ref (read-json (fetch-uri api-host uri)) 'body)))

(define (fetch-activity-details aid)
  (define fmt "https://~a/~a/~a/api/v1/activities/~a?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) aid (api-key)))
  (first (map convert-values! (hash-ref (read-json (fetch-uri api-host uri)) 'body))))

(define (fetch-activities)
  (define fmt "https://~a/~a/~a/api/v1/activities?api_key=~a")
  (define uri (format fmt api-host api-thing (api-username) (api-key)))
  (fetch-all-pages-of uri))

(define (fetch-all-pages-of uri)
  (define answer (read-json (fetch-uri api-host uri)))
  (define lastpage (add1 (hash.refs answer.headers.page_info.total_page)))
  (define list-of-pages 
    (cons (map convert-values! (hash-ref answer 'body)) 
          (for/list ([page (in-range 2 lastpage)]) ; first page was already fetched
            (map convert-values! (hash-ref (read-json (fetch-uri api-host uri page)) 'body)))))
  (apply append list-of-pages))

(define (api-auth-set?)
  (and (api-key) (api-username)))

;; (define (headers-request)
;;   `("Accept: application/json"
;;     ,(format "Authorization: Basic ~a" (auth-basic))))

(define (headers-request-page-info [page 1])
  (list (format "page_info: {\"total_records_per_page\": 500, \"page_number\": ~a}" page)))

(define (make-auth-basic-string user pass)
  (base64-encode (string->bytes/utf-8 (format "~a:~a" user pass)) ""))

;; (define (auth-basic)
;;   (make-auth-basic-string (api-username) (api-password)))

(define (hasheq->hash heq)
  (make-hash (hash->list heq)))

(define (convert-values! heq)
  (define h (hasheq->hash heq))
  (for ([k (hash-keys h)])
    (let ([v (hash-ref h k)])
      (cond [(list? v)
             (hash-set! h k (map convert-values! v))]
            [(and (symbol? v) (symbol=? 'null v)) 
             (hash-set! h k "")]
            [(and (boolean? v) v) 
             (hash-set! h k "True")]
            [(and (boolean? v) (false? v)) 
             (hash-set! h k "False")])))
  h)

(define (api-key-or-fatal-error)
  (or (api-auth-set?) (error 'api-key-or-fatal-error "No API set: impossible to proceed.")))

(define (get-file-extension filename)
  (last (string-split filename ".")))

(define (get-http-code bs)
  (string->number (list-ref (string-split (bytes->string/utf-8 bs)) 1)))

(define api-n-requests (make-parameter 0))
(define (fetch-uri host uri [page 1] [attempt 1])
  (sleep 1)
  (api-n-requests (add1 (api-n-requests)))
  (define-values (codebs headers port) 
    (http-sendrecv host uri 
                   #:ssl? #t 
                   #:headers (headers-request-page-info page)))
  ;; (printf "Request ~a~n" (api-n-requests))
  ;; (when (= (remainder (api-n-requests) 5) 0)
  ;;     (raise (list 'api-non-200-status uri 504 headers (port->bytes port))))
  (define code (get-http-code codebs))
  (cond [(= code 200) port]
        [ ;; in case of 504, let's try 3 times maximum
         (and (= code 504) (<= attempt 3))
         (let ([portbs (port->bytes port)]) 
           (begin
             (printf "attempt #~a failed~n" attempt)
             (printf "Non-200-status: ~a.~n" uri)
             (printf "Code: ~a.~n" code)
             (printf "Response headers: ~a~n" headers)
             (printf "Response body: ~a.~n" portbs)
             (fetch-uri host uri page (add1 attempt))))]
        [else
         (let ([portbs (port->bytes port)]) 
           (begin (printf "Non-200-status: ~a.~n" uri)
                  (printf "Code: ~a.~n" code)
                  (printf "Response headers: ~a~n" headers)
                  (printf "Response body: ~a.~n" portbs)
                  (raise (list 'api-non-200-status uri code headers portbs))))]))

(define repl-cwd-done (make-parameter #f))
(define (repl)
  (when #f
    (current-directory "")
    (repl-cwd-done #t))
  (api-key "")
  (api-username ""))

(provide (all-defined-out)) 
