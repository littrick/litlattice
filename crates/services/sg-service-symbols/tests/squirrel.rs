//! Ports of the baseline squirrel tests:
//! `local_code_intel_test.go`, `hover_test.go` and `service_test.go`.

use regex::Regex;
use sg_service_symbols::squirrel::{self, FileSource, RepoCommitPath, Service};
use std::collections::HashMap;

/// In-memory `readFile`.
struct MemFiles {
    files: HashMap<String, Vec<u8>>,
}

impl MemFiles {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    fn with(mut self, path: &str, contents: &str) -> Self {
        self.files
            .insert(path.to_owned(), contents.as_bytes().to_vec());
        self
    }
}

impl FileSource for MemFiles {
    fn read_file(&self, path: &RepoCommitPath) -> Result<Vec<u8>, String> {
        self.files
            .get(&path.path)
            .cloned()
            .ok_or_else(|| format!("path {} not found", path.path))
    }
}

#[derive(Clone, Debug)]
struct Ann {
    repo: String,
    commit: String,
    path: String,
    row: u32,
    column: u32,
    symbol: String,
    tags: Vec<String>,
}

fn annotation(rcp: &RepoCommitPath, row: u32, column: u32, symbol: &str, tags: &str) -> Ann {
    Ann {
        repo: rcp.repo.clone(),
        commit: rcp.commit.clone(),
        path: rcp.path.clone(),
        row,
        column,
        symbol: symbol.to_owned(),
        tags: tags.split(',').map(str::to_owned).collect(),
    }
}

fn collect_annotations(rcp: &RepoCommitPath, contents: &str) -> Vec<Ann> {
    let mut annotations = Vec::new();
    let lines: Vec<&str> = contents.split('\n').collect();

    // Annotation at the end of the line: < "x" x def
    let end_of_line =
        Regex::new(r#"([^<]+)< "([^"]+)" ([a-zA-Z0-9_.-/]+) ([a-z,]+)"#).expect("regex");
    for (row, line) in lines.iter().enumerate() {
        for captures in end_of_line.captures_iter(line) {
            let substr = captures.get(2).expect("group 2").as_str();
            let symbol = captures.get(3).expect("group 3").as_str();
            let tags = captures.get(4).expect("group 4").as_str();
            let column = line.find(substr).expect("substring");
            annotations.push(annotation(rcp, row as u32, column as u32, symbol, tags));
        }
    }

    let below = Regex::new(r"([^^]*)\^+ ([a-zA-Z0-9_.-/]+) ([a-z,]+)").expect("regex");
    // Annotations below source lines
    let mut source_line: usize = 0;
    'outer: loop {
        let mut ann_line = source_line + 1;
        loop {
            if ann_line >= lines.len() {
                break 'outer;
            }
            let Some(captures) = below.captures(lines[ann_line]) else {
                source_line = ann_line;
                continue 'outer;
            };
            let prefix = captures.get(1).expect("group 1").as_str();
            let symbol = captures.get(2).expect("group 2").as_str();
            let tags = captures.get(3).expect("group 3").as_str();
            let column = spaces_to_column(lines[source_line], length_in_spaces(prefix));
            annotations.push(annotation(
                rcp,
                source_line as u32,
                column as u32,
                symbol,
                tags,
            ));
            ann_line += 1;
        }
    }

    let above = Regex::new(r"([^v]*)v+ ([a-zA-Z0-9_.-/]+) ([a-z,]+)").expect("regex");
    // Annotations above source lines
    let mut source_line = lines.len() as isize - 1;
    'outer: loop {
        let mut ann_line = source_line - 1;
        loop {
            if ann_line < 0 {
                break 'outer;
            }
            let line = lines[ann_line as usize];
            let Some(captures) = above.captures(line) else {
                source_line = ann_line;
                continue 'outer;
            };
            let prefix = captures.get(1).expect("group 1").as_str();
            let symbol = captures.get(2).expect("group 2").as_str();
            let tags = captures.get(3).expect("group 3").as_str();
            let column = spaces_to_column(lines[source_line as usize], length_in_spaces(prefix));
            annotations.push(annotation(
                rcp,
                source_line as u32,
                column as u32,
                symbol,
                tags,
            ));
            ann_line -= 1;
        }
    }

    annotations
}

/// Port of `lengthInSpaces`.
fn length_in_spaces(s: &str) -> usize {
    s.bytes()
        .map(|byte| if byte == b'\t' { 4 } else { 1 })
        .sum()
}

/// Port of `spacesToColumn`.
fn spaces_to_column(s: &str, column: usize) -> usize {
    let mut total = 0;
    for (index, byte) in s.bytes().enumerate() {
        if total >= column {
            return index;
        }
        total += if byte == b'\t' { 4 } else { 1 };
    }
    total
}

fn sort_annotations(annotations: &mut [Ann]) {
    annotations.sort_by(|a, b| (a.row, a.column, &a.tags).cmp(&(b.row, b.column, &b.tags)));
}

/// Compare only (repo, path, point, tags), like the baseline's
/// `compareAnnotations`.
fn assert_annotations_match(path: &str, want: &[Ann], symbols: &[squirrel::Symbol]) {
    let mut got = Vec::new();
    for symbol in symbols {
        got.push(Ann {
            repo: "foo".to_owned(),
            commit: "bar".to_owned(),
            path: path.to_owned(),
            row: symbol.def.row,
            column: symbol.def.column,
            symbol: String::new(),
            tags: vec!["def".to_owned()],
        });
        for reference in &symbol.refs {
            got.push(Ann {
                repo: "foo".to_owned(),
                commit: "bar".to_owned(),
                path: path.to_owned(),
                row: reference.row,
                column: reference.column,
                symbol: String::new(),
                tags: vec!["ref".to_owned()],
            });
        }
    }

    let normalize = |annotations: &[Ann]| {
        let mut sorted: Vec<(u32, u32, Vec<String>)> = annotations
            .iter()
            .map(|ann| (ann.row, ann.column, ann.tags.clone()))
            .collect();
        sorted.sort();
        sorted
    };

    let mut want = want.to_vec();
    sort_annotations(&mut want);
    sort_annotations(&mut got);

    assert_eq!(
        normalize(&want),
        normalize(&got),
        "unexpected annotations for {path}"
    );
}

#[test]
fn local_code_intel_annotations_match_the_baseline() {
    for &(path, contents) in LOCAL_CODE_INTEL_CASES {
        let rcp = RepoCommitPath {
            repo: "foo".to_owned(),
            commit: "bar".to_owned(),
            path: path.to_owned(),
        };
        let files = MemFiles::new().with(path, contents);
        let service = Service::new(&files, None);
        let symbols = service
            .local_code_intel(rcp.clone())
            .unwrap_or_else(|err| panic!("{path}: {err}"));

        assert_annotations_match(path, &collect_annotations(&rcp, contents), &symbols);
    }
}

#[test]
fn local_code_intel_hovers_include_preceding_comments() {
    let cases = [
        ("test.java", JAVA_HOVER, "comment line 1\ncomment line 2\n"),
        ("test.go", GO_HOVER, "comment line 1\ncomment line 2\n"),
        ("test.cs", CSHARP_HOVER, "comment line 1\ncomment line 2\n"),
    ];

    let files = cases
        .iter()
        .fold(MemFiles::new(), |files, (path, contents, _)| {
            files.with(path, contents)
        });
    let service = Service::new(&files, None);

    for (path, _, want) in cases {
        let payload = service
            .local_code_intel(RepoCommitPath {
                repo: "foo".to_owned(),
                commit: "bar".to_owned(),
                path: path.to_owned(),
            })
            .expect("local code intel");
        assert!(
            payload.iter().any(|symbol| symbol.hover.contains(want)),
            "did not find {want:?} in hovers for {path}: {:?}",
            payload
                .iter()
                .map(|symbol| &symbol.hover)
                .collect::<Vec<_>>()
        );
    }
}

const JAVA_HOVER: &str = r#"
class C {
	void m() {
		// not a comment line

		// comment line 1
		// comment line 2
		int x = 5;
	}
}
"#;

const GO_HOVER: &str = r#"
func main() {
	// not a comment line

	// comment line 1
	// comment line 2
	var x int
}
"#;

const CSHARP_HOVER: &str = r#"
namespace Foo {
    class Bar {
        static void Baz(int p) {
			// not a comment line

			// comment line 1
			// comment line 2
			var x = 5;
		}
	}
}
"#;

const LOCAL_CODE_INTEL_CASES: &[(&str, &str)] = &[
    ("test.java", JAVA_CASE),
    ("test.go", GO_CASE),
    ("test.cs", CSHARP_CASE),
    ("exception.py", PYTHON_EXCEPTION_CASE),
    ("test.py", PYTHON_CASE),
    ("test.js", JAVASCRIPT_CASE),
    ("test.ts", TYPESCRIPT_CASE),
    ("test.cpp", CPP_CASE),
    ("test.rb", RUBY_CASE),
];

const JAVA_CASE: &str = r#"
class Foo {

    //             v f1.p def
    //             v f1.p ref
    void f1(String p) {

        //     v f1.x def
        //     v f1.x ref
        //         v f1.p ref
        String x = p;
    }

    //             v f2.p def
    //             v f2.p ref
    void f2(String p) {

        //     v f2.x def
        //     v f2.x ref
        //         v f2.p ref
        String x = p;
    }
}
"#;

const GO_CASE: &str = r#"
var x = 5

//      v f1.p def
//      v f1.p ref
func f1(p int) {

	//  v f1.x def
	//  v f1.x ref
	var x int

	// v f1.y def
	// v f1.y ref
	_, y := g() // < "_" f1.y def < "_" f1.y ref

	//  v f1.i def
	//  v f1.i ref
	//     v f1.j def
	//     v f1.j ref
	for i, j := range z {

		//          v f1.p ref
		//             v f1.i ref
		//                v f1.j ref
		//                   v f1.x ref
		//                      v f1.y ref
		fmt.Println(p, i, j, x, y)
	}

	//     v f1.x ref
	switch x {
	case 3:
		//  v f1.switch1.x def
		//  v f1.switch1.x ref
		var x int
	}

	select {
	//   v f1.switch2.x def
	//   v f1.switch2.x ref
	case x := <-ch:
	}
}
"#;

const CSHARP_CASE: &str = r#"
namespace Foo {
    class Bar {

        //                  v Baz.p def
        //                  v Baz.p ref
        static void Baz(int p) {

            //  v Baz.x def
            //  v Baz.x ref
            int x = 5;

            //                       v Baz.p ref
            //                          v Baz.x ref
            System.Console.WriteLine(p, x);

            //       v Baz.i def
            //       v Baz.i ref
            for (int i = 0; ; ) { }

			//           v Baz.e def
			//           v Baz.e ref
			foreach (int e in es) { }

            //         v Baz.r def
            //         v Baz.r ref
            using (var r = new StringReader("foo")) { }

			try { }
			//               v Baz.e def
			//               v Baz.e ref
			catch (Exception e) { }
        }
    }
}
"#;

const PYTHON_EXCEPTION_CASE: &str = r#"
def f():
	try:
		pass
	#                   v f.e def
	#                   v f.e ref
	except Exception as e:
		#     v f.e ref
		print(e)
"#;

const PYTHON_CASE: &str = r#"
#     vv f.p1 def
#     vv f.p1 ref
#         vv f.p2 def
#         vv f.p2 ref
#                   vv f.p3 def
#                   vv f.p3 ref
#                               vv f.p4 def
#                               vv f.p4 ref
def f(p1, p2: bool, p3 = False, p4: bool = False):
	#     vv f.p1 ref
	#         vv f.p2 ref
	#             vv f.p3 ref
	#                 vv f.p4 ref
	print(p1, p2, p3, p4)

	x = 5 # < "x" f.x def < "x" f.x ref

	#     v f.x ref
	print(x)

	#   v f.i def
	#   v f.i ref
	for i in range(10):
		#     v f.i ref
		print(i)

	try:
		pass
	#                   v f.e def
	#                   v f.e ref
	except Exception as e:
		#     v f.e ref
		print(e)

	#     v f.j ref
	#           v f.j def
	#           v f.j ref
	print(j for j in range(10))

	#      v f.k ref
	#            v f.k def
	#            v f.k ref
	print([k for k in range(10)])
"#;

const JAVASCRIPT_CASE: &str = r#"
//         vv f.p1 def
//         vv f.p1 ref
//             vv f.p2 def
//             vv f.p2 ref
//                        vv f.p3 def
//                        vv f.p3 ref
const f = (p1, p2 = 3, ...p3) => {
	//          vv f.p1 ref
	//              vv f.p2 ref
	//                  vv f.p3 ref
	console.log(p1, p2, p3)

	//    v f.x def
	//    v f.x ref
	const x = 5

	//       v f.g def
	//       v f.g ref
	function g() {}

	// "g" here should be a reference to the function, but the way locals are modeled isn't sophisticated
	// enough (yet?) to express bindings that also escape their lexical scope.

	//          v f.x ref
	console.log(x, g)

	//       v f.i def
	//       v f.i ref
	for (let i = 0; ; ) {
		//          v f.i ref
		console.log(i)
	}

	try { }
	//     v f.e def
	//     v f.e ref
	catch (e) {
		//          v f.e ref
		console.log(e)
	}
}
"#;

const TYPESCRIPT_CASE: &str = r#"
//         vv f.p1 def
//         vv f.p1 ref
//                      vv f.p2 def
//                      vv f.p2 ref
//                                 vv f.p3 def
//                                 vv f.p3 ref
const f = (p1?: number, p2 = 3, ...p3) => {
	//          vv f.p1 ref
	//              vv f.p2 ref
	//                  vv f.p3 ref
	console.log(p1, p2, p3)

	//    v f.x def
	//    v f.x ref
	const x: number = 5

	//       v f.g def
	//       v f.g ref
	function g() {}

	// "g" here should be a reference to the function, but the way locals are modeled isn't sophisticated
	// enough (yet?) to express bindings that also escape their lexical scope.

	//          v f.x ref
	console.log(x, g)

	//       v f.i def
	//       v f.i ref
	for (let i = 0; ; ) {
		//          v f.i ref
		console.log(i)
	}

	try { }
	//     v f.e def
	//     v f.e ref
	catch (e) {
		//          v f.e ref
		console.log(e)
	}
}
"#;

const CPP_CASE: &str = r#"
//         vv f.p1 def
//         vv f.p1 ref
//                 vv f.p2 def
//                 vv f.p2 ref
//                              vv f.p3 def
//                              vv f.p3 ref
//                                       vv f.p4 def
//                                       vv f.p4 ref
void f(int p1, int p2 = 3, int& p3, int* p4)
{
	//  v f.x def
	//  v f.x ref
    int x;

	//  v f.y def
	//  v f.y ref
    int y = 5;

	//       v f.i def
	//       v f.i ref
    for (int i = 0; ; ) { }

	//       v f.j def
	//       v f.j ref
    for (int j : 3) { }

	//   v f.g def
	//   v f.g ref
	//              v f.a def
	//              v f.a ref
	auto g = [](int a) { };

	//                                   v f.e def
	//                                   v f.e ref
    try { } catch (const std::exception& e) { }
}
"#;

const RUBY_CASE: &str = r#"
//    vv f.p1 def
//    vv f.p1 ref
//        vv f.p2 def
//        vv f.p2 ref
//                 vv f.p3 def
//                 vv f.p3 ref
//                       vv f.p4 def
//                       vv f.p4 ref
def f(p1, p2 = 3, *p3, **p4)
	x = 5 # < "x" f.x ref < "x" f.x def

	#         v f.x2 def
	#         v f.x2 ref
	lambda { |x| 5 }

	#          v f.x3 def
	#          v f.x3 ref
	lambda do |x| 5 end

	#   v f.i def
	#   v f.i ref
	for i in 1..5 do end

	begin
		raise ArgumentError
		#                   v f.e def
		#                   v f.e ref
	rescue ArgumentError => e
		#    v f.e ref
		puts e
	end
end
"#;
