package web

import "testing"

func TestDescriptionHTMLRendersSemanticDescriptionText(t *testing.T) {
	got := string(descriptionHTML("# Source heading\n\nFirst line\nSecond line\n\n---\n\nA **bold** and _quiet_ line."))
	want := "<h2>Source heading</h2><p>First line<br>Second line</p><hr><p>A <strong>bold</strong> and <em>quiet</em> line.</p>"
	if got != want {
		t.Fatalf("description html = %q, want %q", got, want)
	}
}

func TestDescriptionHTMLEscapesSourceText(t *testing.T) {
	got := string(descriptionHTML(`A <script>alert("x")</script> line.`))
	want := `<p>A &lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt; line.</p>`
	if got != want {
		t.Fatalf("description html = %q, want %q", got, want)
	}
}
