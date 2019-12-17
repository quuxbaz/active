#lang racket/base

(require 
 racket/list
 racket/bool
 racket/cmdline
 rackunit
 web-server/servlet
 web-server/servlet-env
 web-server/templates
 web-server/http/basic-auth
 net/http-client net/url
 gregor gregor/time
 json xml
 (prefix-in api: "api.rkt")
 (prefix-in model: "model.rkt"))

(define-values (dispatch url)
  (dispatch-rules
   ;; (("departments") departments->response)
   (("activities") activities->response)
   (("programs") programs->response)
   ;; (("sites") sites->response)
   ;; (("program_types") program-types->response)
   ;; (("categories") categories->response)
   ;; (("subcategories") subcategories->response)
   (("program" (string-arg)) program->response)
   ))

(define (program->response r pid)
  (string->response (jsexpr->string (model:get-program-details pid))))

;; (define (categories->response r)
;;   (string->response (jsexpr->string (model:get-categories-all))))

;; (define (subcategories->response r)
;;   (string->response (jsexpr->string (model:get-subcategories-all))))

(define (programs->response r)
  (string->response (jsexpr->string (model:in-memory-db-get))))

;; (define (sites->response r)
;;   (string->response (jsexpr->string (model:get-sites-all))))

;; (define (program-types->response r)
;;   (string->response (jsexpr->string (model:get-program-types-all))))

(define (activities->response r)
  (string->response (jsexpr->string (model:get-activities-all))))

;; (define (departments->response r)
;;   (string->response (jsexpr->string (model:get-departments-all))))

;; nore more api endpoints ==/==

(define (bytes->response bs type)
  (response/full 200 #"Okay" (current-seconds) (string->bytes/utf-8 type) (headers) (list bs)))

;; key assoc -> value
(define (assoc-val key alist)
  (last (assoc key alist)))

(define (headers)
  ;; Access-Control-Allow-Headers: Content-Type
  ;; Access-Control-Allow-Methods: GET, POST, OPTIONS
  ;; Access-Control-Allow-Origin: *
  (list (make-header #"Access-Control-Allow-Headers" #"Content-Type")
        (make-header #"Access-Control-Allow-Methods" #"GET, POST, OPTIONS")
        (make-header #"Access-Control-Allow-Origin" #"*")))

(define (string->response s)
  (response/full 200 #"Okay" (current-seconds) 
                 TEXT/HTML-MIME-TYPE (headers) (list (string->bytes/utf-8 s))))

(define (menu r) ;; just to remind me how xexpr are written
  (response/xexpr 
   `(div (h2 "Examples")
         (ul 
          (li (a ((href "/index.html")) 
                     "The website"))
          (li (a ((href "/api.html")) 
                     "The API"))))))

(define (file-not-found r)
  (response/xexpr "File not found."))

;; This is used in model:get-connection.  With --demo, we can let
;; other programmers see our work by using a sample database and not
;; updating the database ever.
(define demo? (make-parameter #f))

(define (set-things-up)
  (when (not (api:api-auth-set?))
    (displayln "You must set API credentials. See --help for how to.")
    (exit 0))
  ;; Remember we're not yet a web server, which we'll then switch
  ;; current working directories.  So far we're a regular program.
  (printf "Current working directory: ~a~n" (current-directory))
  (current-directory "root/")
  ;; Now let's all be in the same directory --- us and the webserver.
  ;; Remember the webserver is actually a different thread and
  ;; different threads do have their own process table.  (I guess.)
  (printf "Current working directory: ~a~n" (current-directory))
  (model:database-start-up)
  (if (update?)
      (model:set-up)
      (model:set-up-later)))

(define update? (make-parameter #f))
(define no-browser? (make-parameter #f))
(define tcp-port (make-parameter 1111))

(module+ main
  (file-stream-buffer-mode (current-output-port) 'line)
  (command-line 
   #:once-each
   [("--setup") "Sets things up."
    (begin (exit))]
   [("-k" "--key") key "Sets the API key with the data provider."
    (api:api-key key)
    (displayln (format "API key set."))]
   [("-u" "--username") username "Sets the API username."
    (api:api-username username)
    (displayln (format "API username set."))]
   [("--port") port "Sets the port number."
    (tcp-port (string->number port))
    (displayln (format "TCP port ~a set." (tcp-port)))]
   [("--no-browser") "Don't try to open the local browser."
    (no-browser? #t)]
   [("--update-now") "Update /before/ serving pages."
    (update? #f)])

  (define (main)
    (set-things-up)
    (printf "Now serving at port ~a...~n" (tcp-port))
    (serve/servlet dispatch 
                   #:stateless? #t
                   #:log-file (build-path "logs/httpd.log")
                   #:port (tcp-port)
                   #:ssl? #t
		   #:ssl-cert "server-cert.pem"
		   #:ssl-key "private-key.pem"
                   #:listen-ip (if (no-browser?) #f "127.0.0.1")
                   #:servlet-path "/"
                   #:servlet-regexp #rx""
                   #:extra-files-paths (list (build-path "pub/"))
                   #:servlet-current-directory (build-path "./")
                   #:file-not-found-responder file-not-found
                   #:command-line? (no-browser?)))
  (main))
