import json
import pytest
import ckanext.harvest.model as harvest_model
from ckan import model
from ckan.logic import get_action

from ckanext.dataverse.tests import factories

SOURCE_DICT = {
    "url": "https://data.inrae.fr/api/search?q=*",
    "name": "test-dataverse-harvester",
    "title": "Test Dataverse",
    "notes": "Test Dataverse ",
    "source_type": "test-for-action",
    "frequency": "MANUAL",
    "config": json.dumps({"custom_option": ["a", "b"]})
}


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'harvest_setup', 'clean_queues')
@pytest.mark.ckan_config('ckan.plugins', 'harvest test_action_harvester')
class TestActions:
    id = 1
    id_1 = 2
    id_2 = 3

    def test_source_clear(self):
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        object_ = factories.HarvestObjectObj(
            job=job, source=source, package_id=self.id)

        context = {
            'ignore_auth': True,
            'user': ''
        }
        result = get_action('harvest_source_clear')(
            context, {'id': source.id})

        assert result == {'id': source.id}
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert harvest_model.HarvestJob.get(job.id) is None
        assert harvest_model.HarvestObject.get(object_.id) is None
        assert model.Package.get(self.id) is None

    def test_harvest_source_job_history_clear(self):
        # prepare
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        object_ = factories.HarvestObjectObj(job=job, source=source,
                                             package_id=self.id)

        # execute
        context = {'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = get_action('harvest_source_job_history_clear')(
            context, {'id': source.id})

        # verify
        assert result == {'id': source.id}
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert harvest_model.HarvestJob.get(job.id) is None
        assert harvest_model.HarvestObject.get(object_.id) is None
        dataset_from_db = model.Package.get(self.id)
        assert dataset_from_db, 'is None'
        assert dataset_from_db.id == self.id

    def test_harvest_sources_job_history_clear(self):
        # prepare
        data_dict = SOURCE_DICT.copy()
        source_1 = factories.HarvestSourceObj(**data_dict)
        data_dict['name'] = 'another-source'
        data_dict['url'] = 'http://another-url'
        source_2 = factories.HarvestSourceObj(**data_dict)

        job_1 = factories.HarvestJobObj(source=source_1)
        object_1_ = factories.HarvestObjectObj(job=job_1, source=source_1,
                                               package_id=self.id_1)
        job_2 = factories.HarvestJobObj(source=source_2)
        object_2_ = factories.HarvestObjectObj(job=job_2, source=source_2,
                                               package_id=self.id_2)

        # execute
        context = {'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = get_action('harvest_sources_job_history_clear')(
            context, {})

        # verify
        assert sorted(result, key=lambda item: item['id']) == sorted(
            [{'id': source_1.id}, {'id': source_2.id}], key=lambda item: item['id'])
        source_1 = harvest_model.HarvestSource.get(source_1.id)
        assert source_1
        assert harvest_model.HarvestJob.get(job_1.id) is None
        assert harvest_model.HarvestObject.get(object_1_.id) is None
        dataset_from_db_1 = model.Package.get(self.id_1)
        assert dataset_from_db_1, 'is None'
        assert dataset_from_db_1.id == self.id_1
        source_2 = harvest_model.HarvestSource.get(source_2.id)
        assert source_2
        assert harvest_model.HarvestJob.get(job_2.id) is None
        assert harvest_model.HarvestObject.get(object_2_.id) is None
        dataset_from_db_2 = model.Package.get(self.id_2)
        assert dataset_from_db_2, 'is None'
        assert dataset_from_db_2.id == self.id_2
