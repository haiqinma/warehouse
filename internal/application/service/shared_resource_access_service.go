package service

import (
	"context"
	"fmt"
	"time"

	"github.com/yeying-community/warehouse/internal/domain/sharegrant"
	"github.com/yeying-community/warehouse/internal/infrastructure/repository"
)

// SharedResourceAccessService is the V3 operation-time authorization entry
// point. It does not trust a permissions summary returned to a client.
type SharedResourceAccessService struct {
	repository repository.SharedResourceGrantRepository
}

func NewSharedResourceAccessService(repository repository.SharedResourceGrantRepository) *SharedResourceAccessService {
	return &SharedResourceAccessService{repository: repository}
}

func (s *SharedResourceAccessService) Authorize(ctx context.Context, resourceID, targetUserID, action string, now time.Time) (*sharegrant.Resource, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("shared resource access service is not configured")
	}
	resource, grants, err := s.repository.GetAccessibleGrants(ctx, resourceID, targetUserID)
	if err != nil {
		return nil, err
	}
	if resource == nil || !sharegrant.Allows(grants, action, now) {
		return nil, fmt.Errorf("permission denied")
	}
	return resource, nil
}

// AuthorizeLegacyShare supports APIs that have not yet switched request IDs.
// Authorization remains wholly V3: there is deliberately no legacy fallback.
func (s *SharedResourceAccessService) AuthorizeLegacyShare(ctx context.Context, legacyShareID, targetUserID, action string, now time.Time) (*sharegrant.Resource, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("shared resource access service is not configured")
	}
	resourceID, err := s.repository.GetResourceIDByLegacyShareID(ctx, legacyShareID)
	if err != nil {
		return nil, err
	}
	if resourceID == "" {
		return nil, fmt.Errorf("shared resource migration is incomplete")
	}
	return s.Authorize(ctx, resourceID, targetUserID, action, now)
}

func (s *SharedResourceAccessService) EffectivePermissions(ctx context.Context, resourceID, targetUserID string, now time.Time) (*sharegrant.Resource, string, error) {
	if s == nil || s.repository == nil {
		return nil, "", fmt.Errorf("shared resource access service is not configured")
	}
	resource, grants, err := s.repository.GetAccessibleGrants(ctx, resourceID, targetUserID)
	if err != nil {
		return nil, "", err
	}
	if resource == nil {
		return nil, "", fmt.Errorf("shared resource not found or inaccessible")
	}
	return resource, sharegrant.EffectivePermissions(grants, now).String(), nil
}
