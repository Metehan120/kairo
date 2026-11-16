use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use scraper::{Html, Selector};
use kairo::thread::Runtime;

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
struct Page {
    title: String,
    links: Vec<String>,
}

async fn fetch(address: SocketAddr, path: &str) -> Result<Page, String> {
    let stream = async_net::TcpStream::connect(address)
        .await
        .map_err(|e| e.to_string())?;
    let url = format!("http://{address}{path}");
    let request = http_types::Request::new(
        http_types::Method::Get,
        http_types::Url::parse(&url).map_err(|e| e.to_string())?,
    );
    let mut response = async_h1::connect(stream, request)
        .await
        .map_err(|e| e.to_string())?;
    if response.status() != http_types::StatusCode::Ok {
        return Err(format!("{path}: HTTP {}", response.status()));
    }
    let body = response.body_string().await.map_err(|e| e.to_string())?;
    let document = Html::parse_document(&body);
    let title_selector = Selector::parse("title").unwrap();
    let link_selector = Selector::parse("a[href]").unwrap();
    let title = document
        .select(&title_selector)
        .next()
        .ok_or_else(|| format!("{path}: missing title"))?
        .text()
        .collect::<String>();
    let links = document
        .select(&link_selector)
        .filter_map(|a| a.value().attr("href"))
        .map(str::to_owned)
        .collect();
    Ok(Page { title, links })
}

fn read_request(mut stream: &TcpStream) -> std::io::Result<String> {
    stream.set_read_timeout(Some(TIMEOUT))?;
    let mut request = Vec::new();
    let mut byte = [0; 1];
    while !request.ends_with(b"\r\n\r\n") && request.len() < 8192 {
        stream.read_exact(&mut byte)?;
        request.push(byte[0]);
    }
    let request = String::from_utf8(request).map_err(std::io::Error::other)?;
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(str::to_owned)
        .ok_or_else(|| std::io::Error::other("invalid request"))
}

fn respond(mut stream: TcpStream, path: &str) -> std::io::Result<()> {
    let (status, body) = match path {
        "/" => (
            "200 OK",
            "<html><head><title>Home &amp; Index</title></head><body><a href='/about'>About</a><a href='/news'>News</a></body></html>",
        ),
        "/about" => (
            "200 OK",
            "<html><head><title>About</title></head><body><a href='/'>Home</a></body></html>",
        ),
        "/news" => (
            "200 OK",
            "<html><head><title>News</title></head><body><a href='/about'>About</a><a>Not a link</a></body></html>",
        ),
        _ => ("404 Not Found", "Not found"),
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn accept(listener: &TcpListener, deadline: Instant) -> std::io::Result<TcpStream> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(stream),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e),
        }
    }
}

#[test]
fn scrape_linked_pages_concurrently() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local fixture server");
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || -> std::io::Result<Vec<String>> {
        let deadline = Instant::now() + TIMEOUT;
        let root = accept(&listener, deadline)?;
        let root_path = read_request(&root)?;
        respond(root, &root_path)?;

        // Neither linked page is answered until both requests have arrived.
        let first = accept(&listener, deadline)?;
        let first_path = read_request(&first)?;
        let second = accept(&listener, deadline)?;
        let second_path = read_request(&second)?;
        respond(first, &first_path)?;
        respond(second, &second_path)?;
        Ok(vec![root_path, first_path, second_path])
    });

    let runtime = Runtime::new().thread_count(4).build();
    let (sender, receiver) = mpsc::sync_channel(1);
    let root = runtime.spawn(async move { fetch(address, "/").await });
    let root = receiver_for(
        async move { root.await.unwrap() },
        &runtime,
        sender,
        receiver,
    );
    assert_eq!(
        root,
        Page {
            title: "Home & Index".into(),
            links: vec!["/about".into(), "/news".into()]
        }
    );

    let handles: Vec<_> = root
        .links
        .iter()
        .map(|path| {
            let path = path.clone();
            runtime.spawn(async move { fetch(address, &path).await })
        })
        .collect();
    let (sender, receiver) = mpsc::sync_channel(1);
    runtime.spawn(async move {
        let mut pages = Vec::new();
        for handle in handles {
            pages.push(handle.await.unwrap());
        }
        let _ = sender.send(pages);
    });
    let pages = receiver
        .recv_timeout(TIMEOUT)
        .expect("linked page fetches timed out");
    assert_eq!(
        pages,
        vec![
            Ok(Page {
                title: "About".into(),
                links: vec!["/".into()]
            }),
            Ok(Page {
                title: "News".into(),
                links: vec!["/about".into()]
            }),
        ]
    );
    let mut requests = server
        .join()
        .expect("fixture server panicked")
        .expect("fixture server failed");
    requests.sort();
    assert_eq!(requests, ["/", "/about", "/news"]);
}

fn receiver_for<F>(
    future: F,
    runtime: &Runtime,
    sender: mpsc::SyncSender<Result<Page, String>>,
    receiver: mpsc::Receiver<Result<Page, String>>,
) -> Page
where
    F: std::future::Future<Output = Result<Page, String>> + Send + 'static,
{
    runtime.spawn(async move {
        let _ = sender.send(future.await);
    });
    receiver
        .recv_timeout(TIMEOUT)
        .expect("root page fetch timed out")
        .expect("root page fetch failed")
}
