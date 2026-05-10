package ingest

import (
	"context"
	"fmt"
	"strings"
)

func copyCandidateImages(ctx context.Context, st Store, fetcher Fetcher, storage ImageStorage, candidates []EventCandidate) ([]EventCandidate, []string) {
	if len(candidates) == 0 {
		return candidates, nil
	}
	assetStore, _ := st.(ImageAssetStore)
	updated := append([]EventCandidate(nil), candidates...)
	var warnings []string
	for i := range updated {
		sourceURL := strings.TrimSpace(firstNonEmpty(updated[i].ImageSourceURL, updated[i].ImageURL))
		if sourceURL == "" {
			continue
		}
		if err := validateRemoteImageURL(sourceURL); err != nil {
			warnings = append(warnings, fmt.Sprintf("skip image for %q: %v", updated[i].Summary, err))
			continue
		}
		var cachedAsset ImageAsset
		var hasCachedAsset bool
		if assetStore != nil {
			asset, ok, err := assetStore.LoadImageAsset(ctx, sourceURL)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("load image asset for %q: %v", updated[i].Summary, err))
			} else if ok {
				cachedAsset = asset
				hasCachedAsset = true
			}
		}
		if fetcher == nil || storage == nil {
			if hasCachedAsset {
				applyImageAssetToCandidate(&updated[i], cachedAsset)
			}
			continue
		}
		result, err := fetcher.Fetch(ctx, sourceURL)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("fetch image for %q: %v", updated[i].Summary, err))
			if hasCachedAsset {
				applyImageAssetToCandidate(&updated[i], cachedAsset)
			}
			continue
		}
		asset, err := storage.StoreImage(ctx, sourceURL, result)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("store image for %q: %v", updated[i].Summary, err))
			if hasCachedAsset {
				applyImageAssetToCandidate(&updated[i], cachedAsset)
			}
			continue
		}
		if assetStore != nil {
			if err := assetStore.SaveImageAsset(ctx, asset); err != nil {
				warnings = append(warnings, fmt.Sprintf("save image asset for %q: %v", updated[i].Summary, err))
				if hasCachedAsset {
					applyImageAssetToCandidate(&updated[i], cachedAsset)
				}
				continue
			}
		}
		applyImageAssetToCandidate(&updated[i], asset)
	}
	return updated, warnings
}

func attachExistingCandidateImages(ctx context.Context, st any, candidates []EventCandidate) []EventCandidate {
	assetStore, _ := st.(ImageAssetStore)
	if assetStore == nil || len(candidates) == 0 {
		return candidates
	}
	updated := append([]EventCandidate(nil), candidates...)
	for i := range updated {
		sourceURL := strings.TrimSpace(firstNonEmpty(updated[i].ImageSourceURL, updated[i].ImageURL))
		if sourceURL == "" {
			continue
		}
		asset, ok, err := assetStore.LoadImageAsset(ctx, sourceURL)
		if err != nil || !ok {
			continue
		}
		applyImageAssetToCandidate(&updated[i], asset)
	}
	return updated
}

func applyImageAssetToCandidate(candidate *EventCandidate, asset ImageAsset) {
	if candidate == nil {
		return
	}
	candidate.ImageURL = strings.TrimSpace(asset.PublicURL)
	candidate.ImageSourceURL = strings.TrimSpace(asset.SourceURL)
	candidate.ImageWidth = asset.Width
	candidate.ImageHeight = asset.Height
	candidate.ImageFocusX = NormalizeExplicitImageFocusValue(asset.FocusX)
	candidate.ImageFocusY = NormalizeExplicitImageFocusValue(asset.FocusY)
}
