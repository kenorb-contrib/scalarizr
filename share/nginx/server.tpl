http {
    server {
        listen   80;
    
        if ( $remote_addr = 127.0.0.1 ) {
                rewrite ^(.*)$ /noapp.html last;
                return 302;
        }
        location /noapp.html {
                root /usr/share/nginx/html;
        } 
    
        location /    {
            proxy_pass         http://backend;
            proxy_buffering    on;
    
            proxy_set_header   Host             $host;
            proxy_set_header   X-Real-IP        $remote_addr;
            proxy_set_header   X-Forwarded-For  $proxy_add_x_forwarded_for;
    
            error_page   500 501 /500.html;
            error_page   502 503 504 /502.html;
        }
    
        location /500.html {
        		expires 0;
                root   /usr/share/nginx/html;
        }
    
        location /502.html {
        		expires 0;
                root   /usr/share/nginx/html;
        }
    }
}