library(rvest, quietly = T)
library(httr, quietly = T)
library(data.table, quietly = T)
library(dtplyr, quietly = T)
library(dplyr, quietly = T)
library(RMySQL, quietly = T)

find_last_page <- function(gr) {
  go_next <- TRUE
  next_page <- 1
  
  while(go_next) {
    doc <- read_lchi_page(next_page, gr)
    pnav <- html_nodes(doc, css = '.pnav')
    
    if (length(pnav) > 0) {
      next_page <- gsub("^.+fChPage\\(([0-9]+)\\).+$", "\\1", 
                        pnav[grepl(pattern = "fChPage", pnav)], perl = T) %>% 
        as.integer() %>% unique() %>% max()
      
      go_next <- any(grepl("След", html_text(pnav)))
    } else break
    
  }
  
  next_page
}

read_lchi_page <- function(x, y) {
  url <- "http://investor.moex.com/ru/statistics/2017/default.aspx"
  
  resp <- POST(url, body=list(act = '',
                              sby = 8,
                              nick = '',
                              gr = as.character(y),
                              `data-contype-id-1` = '',
                              `data-contype-id-2` = '',
                              `data-contype-id-3` = '',
                              pge = x), encode="form")
  content(resp)
}

format_number <- function(x) gsub("[[:space:]]", "", gsub(",", ".", x))

parse_lchi_userlist <- function(page, gr) {
  p0 <- read_lchi_page(page, gr) 
  
  xp <- '//table[@class = "table table-bordered table-striped table-stat"]'
  
  p1 <- html_nodes(p0, xpath = xp) %>% html_table()
  
  p1 <- p1[[1]]
  
  p2 <- html_nodes(p0, css = '.nickname') %>% html_nodes("a")
  
  d <- html_nodes(p0, xpath='//select[@id="date"]/option[1]') %>% html_text()

  start_capital <- format_number(p1[,grepl("Стартовые активы", 
                                                      names(p1))])
  income <- format_number(p1[,grepl("Общий доход", names(p1))])
  
  tibble(user = p1$`Участник`, url = html_attr(p2, "href"),
             initial = as.numeric(start_capital), 
             date = as.POSIXct(strptime(d, "%Y-%m-%d")), 
         deals = as.numeric(gsub("-", "0", p1$`Сделок`)), income) %>% 
    filter(deals > 0, income != "-")
}

read_all_ids <- function(gr) {
  ul <- do.call("bind_rows", lapply(seq(1:find_last_page(gr)), function(x)
    parse_lchi_userlist(x, gr)))
  
  unique(ul, by = "user")
}

generate_js <- function(x) {
  js <- paste0("var webPage = require('webpage');
               var page = webPage.create();
               
               var fs = require('fs');
               var path = 'userpage.html'
              
               page.settings.loadImages = false
               
               page.open('http://investor.moex.com", x,"', function (status) {
               var content = page.content;
               fs.write(path,content,'w')
               phantom.exit();
               });")
  f <- file("scrape_lchi.js", "w")
  writeLines(js, con = f)
  close(f)
}

read_user_data <- function(u) {
  generate_js(u$url)
  system("./phantomjs scrape_lchi.js", show.output.on.console = F)

  html_file <- read_html("userpage.html")
  
  spinner <- html_nodes(html_file, 
                        xpath='//span[@spinner-key="spinner-6"]') %>% 
    xml_children()

  df <- NULL
  
  if(length(spinner) == 0) {
    df <- tibble()

    tabs <- html_nodes(html_file, xpath='//div[@class="for_table"]/table') %>% 
      html_table()
    
    if (!is.null(tabs)) {
      if (length(tabs) > 0) {
        deals <- tabs[[length(tabs)]]
        
        colnames(deals) <- c("MARKET", "SEC", "POSITION", "PRICE", "BALANCE", 
                             "COST")
        
        df <- as_tibble(deals) %>% select(SEC, POSITION) %>%
          mutate_at("POSITION", function(x) gsub("^(-?[0-9]\\d*).+$", "\\1", 
                                                 x)) %>% 
          mutate_at("POSITION", as.numeric)
        
        if(nrow(df) > 0) mutate(df, USER = u$user)
      }
    }
  }
  
  df
}

replace_mysql <- function(con, tblname, dump, ch) {
  for (i in 1:ceiling(nrow(dump)/ch)) {
    query = paste0('REPLACE INTO ', tblname, '(',
                   paste0('`', colnames(dump), '`', collapse = ','),') VALUES ')
    vals = NULL
    for (j in 1:ch) {
      k = (i-1)*ch+j
      if (k <= nrow(dump)) {
        vals[j] = paste0('(', paste0('"', dump[k,], '"', collapse = ','), ')')
      }
    }
    query = paste0(query, paste0(vals,collapse=','))
    
    print(query)
    dbExecute(con, query)
  }
}

upload_to_mysql <- function(raw) {
  con <- dbConnect(RMySQL::MySQL(),
                   host = "general.c7rqzkms4qhi.us-east-2.rds.amazonaws.com",
                   dbname = "",
                   password = "",
                   user = "")
  
  df <- mutate(raw, LONG = ifelse(POSITION > 0, POSITION, 0), 
         SHORT = ifelse(POSITION < 0, POSITION, 0)) %>% group_by(SEC, DATE) %>% 
    summarize(LONG = sum(LONG), SHORT = sum(SHORT)) %>%
    filter(SHORT < 0) %>% mutate(LS_RATIO = round(LONG/(abs(LONG)+abs(SHORT)),
                                                  2))
    
  
  replace_mysql(con, "lchi", mutate_at(df, "DATE", function(x) 
    strftime(x, "%Y-%m-%d")), ch = 10)
  
  dbDisconnect(con)
}

read_n_users <- function(ids = NULL, n = NULL) {
  datalist = list()
  
  if (is.null(ids)) ids <- read_all_ids(12)
  if (is.null(n)) n <- nrow(ids)

  ids <- ids[1:n,]
  
  idx <- 0
  while(idx < n) {
    r <- ids[1, ]
    dat <- read_user_data(r)
    if (is.null(dat)) ids <- rbind(ids, r)
    else {
      idx <- idx + 1
      print(paste0(idx, "/", n, ": ", r$user, ": ",  r$url))
      if(nrow(dat)>0) {
        dat$INITIAL <- r$initial
        dat$DATE <- r$date
      }
      datalist[[idx]] <- dat
    }
    ids <- ids[-1,]
  }
  
  do.call(bind_rows, datalist)
}

do_job <- function() {
  df <- read_n_users()
  upload_to_mysql(df)  
}