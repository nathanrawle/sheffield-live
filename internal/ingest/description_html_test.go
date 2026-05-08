package ingest

import "testing"

func TestSemanticDescriptionTextPreservesStructuralBreaks(t *testing.T) {
	raw := `<h2>About the night</h2><p>First line<br>Second line<br><br>Next paragraph</p><hr><p>Final paragraph</p>`

	got := semanticDescriptionText(raw)
	want := "## About the night\n\nFirst line\nSecond line\n\nNext paragraph\n\n---\n\nFinal paragraph"
	if got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestSemanticDescriptionTextConveysEmphasisAndDropsStyle(t *testing.T) {
	raw := `<p style="font-size:30px;color:red">A <strong>bold</strong> and <em>quiet</em> line.</p><style>.x{color:red}</style><p><a style="color:blue" href="https://example.test">Ticket text</a></p>`

	got := semanticDescriptionText(raw)
	want := "A **bold** and _quiet_ line.\n\nTicket text"
	if got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestSemanticDescriptionTextPreservesInlineTagBoundaryWhitespace(t *testing.T) {
	raw := `<p>Known for supporting artists ranging from <strong>Sleater-Kinney </strong>and <strong>The Breeders </strong>to <strong>The Cribs </strong>and <strong>Stereophonics.</strong></p>`

	got := semanticDescriptionText(raw)
	want := "Known for supporting artists ranging from **Sleater-Kinney** and **The Breeders** to **The Cribs** and **Stereophonics.**"
	if got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestSemanticDescriptionTextCollapsesRepeatedBreaks(t *testing.T) {
	raw := `One<br><br><br>Two<br />Three`

	got := semanticDescriptionText(raw)
	want := "One\n\nTwo\nThree"
	if got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}
