from rest_framework import viewsets
from rest_api.serializers import *
from rest_api.models import *

class CoFacilityViewSet(viewsets.ModelViewSet):
    queryset = CoFacility.objects.all().order_by('-std_day')
    serializer_class = CoFacilitySerializers
    # permission_classes = [permissions.IsAuthenticated]

class CoPopuDensityViewSet(viewsets.ModelViewSet):
    queryset = CoPopuDensity.objects.all().order_by('-std_day')
    serializer_class = CoPopuDensitySerializers
    # permission_classes = [permissions.IsAuthenticated]

class CoVaccineViewSet(viewsets.ModelViewSet):
    queryset = CoVaccine.objects.all().order_by('-std_day')
    serializer_class = CoVaccineSerializers
    # permission_classes = [permissions.IsAuthenticated]

class CoWeekdayViewSet(viewsets.ModelViewSet):
    queryset = CoWeekday.objects.all().order_by('-std_day')
    serializer_class = CoWeekdaySerializers
    # permission_classes = [permissions.IsAuthenticated]




